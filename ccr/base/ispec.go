package base

import "database/sql"

type ISpec interface {
	Valid() error
	Connect() (*sql.DB, error)
	ConnectDB() (*sql.DB, error)
	IsDatabaseEnableBinlog() (bool, error)
	IsTableEnableBinlog() (bool, error)
	GetAllTables() ([]string, error)
	DropTable() error
	DropTables(tables []string) ([]string, error)
	ClearDB() error
	CreateDatabase() error
	CreateTable(stmt string) error
	CheckDatabaseExists() (bool, error)
	CheckTableExists() (bool, error)
	CreateSnapshotAndWaitForDone(tables []string) (string, error)
	CheckRestoreFinished(snapshotName string) (bool, error)

	Exec(sql string) error
	DbExec(sql string) error
}
