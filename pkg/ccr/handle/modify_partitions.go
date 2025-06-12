package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.BatchModifyPartitionsInfo](festruct.TBinlogType_MODIFY_PARTITIONS, &ModifyPartitionsHandle{})
}

type ModifyPartitionsHandle struct {
	// The modify partitions binlog is idempotent
	IdempotentJobHandle[*record.BatchModifyPartitionsInfo]
}

func (h *ModifyPartitionsHandle) Handle(j *ccr.Job, commitSeq int64, batchModifyPartitionsInfo *record.BatchModifyPartitionsInfo) error {
	// TODO: custom by medium_sync_policy
	if !ccr.FeatureMediumSyncPolicy || j.MediumSyncPolicy == "hdd" {
		log.Warnf("skip modify partitions for FeatureMediumSyncPolicy off or medium_sync_policy is hdd")
		return nil
	}

	// Safety check: ensure we have partition infos to process
	if batchModifyPartitionsInfo == nil || len(batchModifyPartitionsInfo.Infos) == 0 {
		return xerror.Errorf(xerror.Normal, "batch modify partitions info is empty or nil")
	}

	// Get table ID from the first partition info (all partitions should belong to the same table)
	tableId := batchModifyPartitionsInfo.GetTableId()
	if tableId <= 0 {
		return xerror.Errorf(xerror.Normal, "invalid table ID: %d", tableId)
	}

	// Check if it's a materialized view table
	if isAsyncMv, err := j.IsMaterializedViewTable(tableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip modify partitions for materialized view table %d", tableId)
		return nil
	}

	// Get destination table name
	destTableName, err := j.GetDestNameBySrcId(tableId)
	if err != nil {
		return err
	}

	// Get the source cluster meta information and supplement the partition name information
	srcMeta := j.GetSrcMeta()
	if err := batchModifyPartitionsInfo.EnrichWithPartitionNames(srcMeta); err != nil {
		log.Errorf("failed to enrich partition names from source meta: %v", err)
		return err
	}

	// Call spec layer method directly
	return j.Dest.ModifyPartitionProperty(destTableName, batchModifyPartitionsInfo)
}
