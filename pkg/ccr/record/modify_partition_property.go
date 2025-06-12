package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

type DataProperty struct {
	StorageMedium          string `json:"storageMedium"`
	CooldownTimeMs         int64  `json:"cooldownTimeMs"`
	StoragePolicy          string `json:"storagePolicy"`
	IsMutable              bool   `json:"isMutable"`
	StorageMediumSpecified bool   `json:"storageMediumSpecified,omitempty"`
}

type Tag struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ReplicaAllocation struct {
	AllocMap map[string]int16 `json:"allocMap"`
}

// ModifyPartitionInfo represents single partition modification info
type ModifyPartitionInfo struct {
	DbId           int64              `json:"dbId"`
	TableId        int64              `json:"tableId"`
	PartitionId    int64              `json:"partitionId"`
	DataProperty   *DataProperty      `json:"dataProperty"`
	ReplicationNum int16              `json:"replicationNum"`
	IsInMemory     bool               `json:"isInMemory"`
	ReplicaAlloc   *ReplicaAllocation `json:"replicaAlloc"`
	StoragePolicy  string             `json:"storagePolicy"`
	TblProperties  map[string]string  `json:"tableProperties"`
	// PartitionName is not from Binlog:ModifyPartitionInfo, it's used for cross-cluster partition mapping
	PartitionName string `json:"partitionName,omitempty"`
}

type BatchModifyPartitionsInfo struct {
	Infos []*ModifyPartitionInfo `json:"infos"`
}

func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &batchModifyPartitionsInfo)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal batch modify partitions info error")
	}

	if len(batchModifyPartitionsInfo.Infos) == 0 {
		return xerror.Errorf(xerror.Normal, "modify partition infos is empty")
	}

	return nil
}

func NewBatchModifyPartitionsInfoFromJson(data string) (*BatchModifyPartitionsInfo, error) {
	var batchModifyPartitionsInfo BatchModifyPartitionsInfo
	if err := batchModifyPartitionsInfo.Deserialize(data); err != nil {
		return nil, err
	}
	return &batchModifyPartitionsInfo, nil
}

func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) String() string {
	return fmt.Sprintf("BatchModifyPartitionsInfo: Infos count: %d", len(batchModifyPartitionsInfo.Infos))
}

// GetTableId implements Record interface by returning the first table ID in the batch
func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) GetTableId() int64 {
	if len(batchModifyPartitionsInfo.Infos) == 0 {
		// This should not happen, because the Infos should not be empty after successful deserialization
		log.Warnf("BatchModifyPartitionsInfo.Infos is empty, this should not happen after successful deserialization")
		return -1 // return -1 to indicate invalid TableId
	}
	return batchModifyPartitionsInfo.Infos[0].TableId
}

// GetPartitionName returns the partition name for cross-cluster mapping
func (info *ModifyPartitionInfo) GetPartitionName() string {
	return info.PartitionName
}

// Metaer interface for accessing partition metadata
type Metaer interface {
	GetPartitionName(tableId int64, partitionId int64) (string, error)
}

// EnrichWithPartitionNames enriches the batch with partition name information from source cluster meta
func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) EnrichWithPartitionNames(srcMeta Metaer) error {
	for _, partitionInfo := range batchModifyPartitionsInfo.Infos {
		// If the partition name information is already in the binlog, skip
		if partitionInfo.PartitionName != "" {
			continue
		}

		// Get the partition name from the source cluster meta
		partitionName, err := srcMeta.GetPartitionName(partitionInfo.TableId, partitionInfo.PartitionId)
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal,
				"failed to get partition name for table %d partition %d",
				partitionInfo.TableId, partitionInfo.PartitionId)
		}

		partitionInfo.PartitionName = partitionName
	}

	return nil
}
