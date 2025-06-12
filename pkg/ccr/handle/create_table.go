package handle

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.CreateTable](festruct.TBinlogType_CREATE_TABLE, &CreateTableHandle{})
}

type CreateTableHandle struct {
	IdempotentJobHandle[*record.CreateTable]
}

// Check if error message indicates storage medium or capacity related issues
func isStorageMediumError(errMsg string) bool {
	log.Infof("STORAGE_MEDIUM_DEBUG: Analyzing error message: %s", errMsg)

	patterns := []string{
		"capExceedLimit",
		"Failed to find enough backend",
		"not enough backend",
		"storage medium",
		"storage_medium",
		"avail capacity",
		"disk space",
		"not enough space",
		"replication num",
		"replication tag",
	}

	for _, pattern := range patterns {
		if strings.Contains(strings.ToLower(errMsg), strings.ToLower(pattern)) {
			log.Infof("STORAGE_MEDIUM_DEBUG: Found storage/capacity related pattern '%s' in error message", pattern)
			return true
		}
	}

	log.Infof("STORAGE_MEDIUM_DEBUG: No storage/capacity related patterns found in error message")
	return false
}

// Extract storage_medium from CREATE TABLE SQL
func extractStorageMediumFromCreateTableSql(createSql string) string {
	pattern := `"storage_medium"\s*=\s*"([^"]*)"`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(createSql)
	if len(matches) >= 2 {
		medium := strings.ToLower(matches[1])
		log.Infof("STORAGE_MEDIUM_DEBUG: Extracted storage medium: %s", medium)
		return medium
	}
	log.Infof("STORAGE_MEDIUM_DEBUG: No storage medium found in SQL")
	return ""
}

// Switch storage medium between SSD and HDD
func switchStorageMedium(medium string) string {
	switch strings.ToLower(medium) {
	case "ssd":
		return "hdd"
	case "hdd":
		return "ssd"
	default:
		// Default to hdd if not standard medium
		return "hdd"
	}
}

// Set specific storage_medium in CREATE TABLE SQL
func setStorageMediumInCreateTableSql(createSql string, medium string) string {
	// Remove existing storage_medium first
	createSql = ccr.FilterStorageMediumFromCreateTableSql(createSql)

	// Check if PROPERTIES clause exists
	propertiesPattern := `PROPERTIES\s*\(`
	if matched, _ := regexp.MatchString(propertiesPattern, createSql); matched {
		// Add storage_medium at the beginning of PROPERTIES
		pattern := `(PROPERTIES\s*\(\s*)`
		replacement := fmt.Sprintf(`${1}"storage_medium" = "%s", `, medium)
		createSql = regexp.MustCompile(pattern).ReplaceAllString(createSql, replacement)
	} else {
		// Add entire PROPERTIES clause
		pattern := `(\s*)$`
		replacement := fmt.Sprintf(` PROPERTIES ("storage_medium" = "%s")`, medium)
		createSql = regexp.MustCompile(pattern).ReplaceAllString(createSql, replacement)
	}

	return createSql
}

// Process CREATE TABLE SQL according to medium sync policy
func processCreateTableSqlByMediumPolicy(j *ccr.Job, createTable *record.CreateTable) error {
	// Note: We need to access Job's medium sync policy and feature flags
	// For now, we'll implement basic logic based on what we know the Job should do

	// Check if medium sync policy feature is enabled (we assume it's enabled for new handler)
	// This is a simplified version that handles the main cases
	mediumPolicy := j.MediumSyncPolicy

	switch mediumPolicy {
	case ccr.MediumSyncPolicySameWithUpstream:
		// Keep upstream storage_medium unchanged
		log.Infof("using same_with_upstream policy, keeping original storage_medium")
		return nil

	case ccr.MediumSyncPolicyHDD:
		// Force set to HDD
		log.Infof("using hdd policy, setting storage_medium to hdd")
		createTable.Sql = setStorageMediumInCreateTableSql(createTable.Sql, "hdd")
		return nil

	default:
		log.Warnf("unknown medium sync policy: %s, falling back to filter storage_medium", mediumPolicy)
		if ccr.FeatureFilterStorageMedium {
			createTable.Sql = ccr.FilterStorageMediumFromCreateTableSql(createTable.Sql)
		}
		return nil
	}
}

// Create table with medium retry mechanism
func createTableWithMediumRetry(j *ccr.Job, createTable *record.CreateTable, srcDb string) error {
	originalSql := createTable.Sql
	log.Infof("STORAGE_MEDIUM_DEBUG: Starting create table with medium retry for table: %s", createTable.TableName)

	// Process SQL according to medium policy
	if err := processCreateTableSqlByMediumPolicy(j, createTable); err != nil {
		return err
	}

	// First attempt
	err := j.IDest.CreateTableOrView(createTable, srcDb)
	if err == nil {
		log.Infof("STORAGE_MEDIUM_DEBUG: Create table succeeded on first attempt")
		return nil
	}

	log.Warnf("STORAGE_MEDIUM_DEBUG: First attempt failed: %s", err.Error())

	// Check if it's storage related error and should retry
	if !isStorageMediumError(err.Error()) {
		log.Infof("STORAGE_MEDIUM_DEBUG: Not a storage related error, no retry")
		return err
	}

	// Extract current medium and switch to the other one
	currentMedium := extractStorageMediumFromCreateTableSql(createTable.Sql)
	if currentMedium == "" {
		currentMedium = "ssd" // default
	}

	switchedMedium := switchStorageMedium(currentMedium)
	log.Infof("STORAGE_MEDIUM_DEBUG: Switching from %s to %s", currentMedium, switchedMedium)

	createTable.Sql = setStorageMediumInCreateTableSql(originalSql, switchedMedium)

	// Second attempt with switched medium
	err = j.IDest.CreateTableOrView(createTable, srcDb)
	if err == nil {
		log.Infof("STORAGE_MEDIUM_DEBUG: Create table succeeded after switching to %s", switchedMedium)
		return nil
	}

	log.Warnf("STORAGE_MEDIUM_DEBUG: Second attempt with %s also failed: %s", switchedMedium, err.Error())

	// Final attempt: remove storage_medium if still storage related error
	if isStorageMediumError(err.Error()) {
		log.Infof("STORAGE_MEDIUM_DEBUG: Removing storage_medium for final attempt")
		createTable.Sql = ccr.FilterStorageMediumFromCreateTableSql(originalSql)

		err = j.IDest.CreateTableOrView(createTable, srcDb)
		if err == nil {
			log.Infof("STORAGE_MEDIUM_DEBUG: Create table succeeded after removing storage_medium")
			return nil
		}

		log.Warnf("STORAGE_MEDIUM_DEBUG: Final attempt without storage_medium also failed: %s", err.Error())
	}

	log.Errorf("STORAGE_MEDIUM_DEBUG: All attempts failed, returning final error")
	return err
}

func (h *CreateTableHandle) Handle(j *ccr.Job, commitSeq int64, createTable *record.CreateTable) error {
	if j.SyncType != ccr.DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	if createTable.IsCreateElasticSearch() {
		log.Warnf("create table with elasticsearch is not supported yet, skip this binlog")
		return nil
	}

	if createTable.IsCreateMaterializedView() {
		log.Warnf("create async materialized view is not supported yet, skip this binlog")
		return nil
	}

	if ccr.FeatureCreateViewDropExists {
		tableName := strings.TrimSpace(createTable.TableName)
		if createTable.IsCreateView() && len(tableName) > 0 {
			// drop view if exists
			log.Infof("feature_create_view_drop_exists is enabled, try drop view %s before creating", tableName)
			if err := j.IDest.DropView(tableName); err != nil {
				return xerror.Wrapf(err, xerror.Normal, "drop view before create view %s, table id=%d",
					tableName, createTable.TableId)
			}
		}
	}

	if createTable.IsCreateTableWithInvertedIndex() {
		log.Infof("create table %s with inverted index, force partial snapshot, commit seq : %d", createTable.TableName, commitSeq)
		// we need to force replace table to ensure the index id is consistent
		return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, true, false)
	}

	// Some operations, such as DROP TABLE, will be skiped in the partial/full snapshot,
	// in that case, the dest table might already exists, so we need to check it before creating.
	// If the dest table already exists, we need to do a partial snapshot.
	//
	// See test_cds_fullsync_tbl_drop_create.groovy for details
	if j.SyncType == ccr.DBSync && !createTable.IsCreateView() {
		if exists, err := j.IDest.CheckTableExistsByName(createTable.TableName); err != nil {
			return err
		} else if exists {
			log.Warnf("the dest table %s already exists, force partial snapshot, commit seq: %d",
				createTable.TableName, commitSeq)
			replace := true
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
	}

	// Remove old storage_medium filtering logic, handled by new retry function
	createTable.Sql = ccr.FilterDynamicPartitionStoragePolicyFromCreateTableSql(createTable.Sql)

	// Use new create table function with medium retry mechanism
	if err := createTableWithMediumRetry(j, createTable, j.Src.Database); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Can not found function") {
			log.Warnf("skip creating table/view because the UDF function is not supported yet: %s", errMsg)
			return nil
		} else if strings.Contains(errMsg, "Can not find resource") {
			log.Warnf("skip creating table/view for the resource is not supported yet: %s", errMsg)
			return nil
		} else if createTable.IsCreateView() && strings.Contains(errMsg, "Unknown column") {
			log.Warnf("create view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				commitSeq, errMsg)
			replace := false // new view no need to replace
			isView := true
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		if len(createTable.TableName) > 0 && ccr.IsSessionVariableRequired(errMsg) { // ignore doris 2.0.3
			log.Infof("a session variable is required to create table %s, force partial snapshot, commit seq: %d, msg: %s",
				createTable.TableName, commitSeq, errMsg)
			replace := false // new table no need to replace
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		return xerror.Wrapf(err, xerror.Normal, "create table %d", createTable.TableId)
	}

	j.GetSrcMeta().ClearTablesCache()
	j.GetDestMeta().ClearTablesCache()

	var srcTableName string

	srcTableName = createTable.TableName
	if len(srcTableName) == 0 {
		// the field `TableName` is added after doris 2.0.3, to keep compatible, try read src table
		// name from upstream, but the result might be wrong if upstream has executed rename/replace.
		log.Infof("the table id %d is not found in the binlog record, get the name from the upstream", createTable.TableId)
		var err error
		srcTableName, err = j.GetSrcMeta().GetTableNameById(createTable.TableId)
		if err != nil {
			return xerror.Errorf(xerror.Normal, "the table with id %d is not found in the upstream cluster, create table: %s",
				createTable.TableId, createTable.String())
		}
	}

	job_progress := j.GetJobProgress()
	destTableId, err := j.GetDestMeta().GetTableId(srcTableName)
	if err != nil {
		return err
	}

	if job_progress.TableMapping == nil {
		job_progress.TableMapping = make(map[int64]int64)
	}
	job_progress.TableMapping[createTable.TableId] = destTableId
	if job_progress.TableNameMapping == nil {
		job_progress.TableNameMapping = make(map[int64]string)
	}
	job_progress.TableNameMapping[createTable.TableId] = srcTableName
	return nil
}
