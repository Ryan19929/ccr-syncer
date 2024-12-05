// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_ds_prop_incrsync_incsync_tm_compact") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameFull = "tbl_full"
    def tableNameIncrement = "tbl_incr"
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}"


    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameFull}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "compaction_policy" = "time_series",
            "time_series_compaction_goal_size_mbytes" = "1024",
            "time_series_compaction_file_count_threshold" = "2000",
            "time_series_compaction_time_threshold_seconds" = "3600",
            "time_series_compaction_level_threshold" = "2"
        )
    """

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "target"))

    def res = sql "SHOW CREATE TABLE ${tableNameFull}"

    def target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}"

    assertTrue(target_res[0][1].contains("\"compaction_policy\" = \"time_series\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_goal_size_mbytes\" = \"1024\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_file_count_threshold\" = \"2000\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_time_threshold_seconds\" = \"3600\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_level_threshold\" = \"2\""))

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrement}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "compaction_policy" = "time_series",
            "time_series_compaction_goal_size_mbytes" = "1024",
            "time_series_compaction_file_count_threshold" = "2000",
            "time_series_compaction_time_threshold_seconds" = "3600",
            "time_series_compaction_level_threshold" = "2"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, "target"))

    res = sql "SHOW CREATE TABLE ${tableNameIncrement}"

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}"

    assertTrue(target_res[0][1].contains("\"compaction_policy\" = \"time_series\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_goal_size_mbytes\" = \"1024\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_file_count_threshold\" = \"2000\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_time_threshold_seconds\" = \"3600\""))
    assertTrue(target_res[0][1].contains("\"time_series_compaction_level_threshold\" = \"2\""))
}