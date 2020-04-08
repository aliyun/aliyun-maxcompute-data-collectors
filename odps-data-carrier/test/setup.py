# Copyright 1999-2019 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import random
import string
import time
import traceback

import mma_test.utils as utils

from concurrent.futures import ThreadPoolExecutor


def init_database():
    _, _ = utils.execute_command("hive -f %s" % os.path.join(utils.test_dir, "setup.sql"))


def prepare_data():

    # util methods to generate partition
    def random_string(length: int):
        return "".join(random.choices(string.ascii_letters, k=length))

    def random_bigint():
        return random.randint(0, 9999)

    def insert_overwrite_non_partitioned_tbl_statement(table: str, num_records: int):
        return ("insert overwrite table MMA_TEST.%s "
                "select extend_table(%d) from MMA_TEST.DUMMY") % (table, num_records)

    def insert_overwrite_partitioned_tbl_statement(table: str,
                                                   partition_value_1: str,
                                                   partition_value_2: int,
                                                   number_records: int):
        template = ("insert overwrite table MMA_TEST.%s partition(p1='%s', p2=%d)"
                    "select extend_table(%d) from MMA_TEST.DUMMY")
        return template % (table, partition_value_1, partition_value_2, number_records)

    local_udtf_path = os.path.join(utils.odps_data_carrier_dir,
                                   "lib",
                                   "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar")
    hdfs_udtf_path = "hdfs:///mma/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    class_name = "com.aliyun.odps.datacarrier.transfer.RandomDataGenerateUDTF"

    setup_commands = [
        "dfs -mkdir -p /mma;",
        "dfs -put -f %s %s;" % (local_udtf_path, hdfs_udtf_path),
        "drop function if exists extend_table;"
        "create function extend_table as '%s' using jar '%s';" % (class_name, hdfs_udtf_path),
    ]

    commands = [
        insert_overwrite_non_partitioned_tbl_statement("TEST_TEXT_1x1K", 1000),
        insert_overwrite_non_partitioned_tbl_statement("TEST_ORC_1x1K", 1000),
        insert_overwrite_non_partitioned_tbl_statement("TEST_PARQUET_1x1K", 1000),
        insert_overwrite_non_partitioned_tbl_statement("TEST_RCFILE_1x1K", 1000),
        insert_overwrite_non_partitioned_tbl_statement("TEST_SEQUENCEFILE_1x1K", 1000),
        insert_overwrite_non_partitioned_tbl_statement("TEST_NON_PARTITIONED_1x100K", 100000),
    ]

    for i in range(10):
        commands.append(insert_overwrite_partitioned_tbl_statement("TEST_TEXT_PARTITIONED_10x1K",
                                                                   random_string(5),
                                                                   random_bigint(),
                                                                   1000))
        commands.append(insert_overwrite_partitioned_tbl_statement("TEST_ORC_PARTITIONED_10x1K",
                                                                   random_string(5),
                                                                   random_bigint(),
                                                                   1000))
        commands.append(insert_overwrite_partitioned_tbl_statement(
            "TEST_PARQUET_PARTITIONED_10x1K", random_string(5), random_bigint(), 1000))
        commands.append(insert_overwrite_partitioned_tbl_statement(
            "TEST_RCFILE_PARTITIONED_10x1K", random_string(5), random_bigint(), 1000))
        commands.append(insert_overwrite_partitioned_tbl_statement(
            "TEST_SEQUENCEFILE_PARTITIONED_10x1K", random_string(5), random_bigint(), 1000))

    for i in range(100):
        commands.append(insert_overwrite_partitioned_tbl_statement(
            "TEST_PARTITIONED_100x10K", random_string(5), random_bigint(), 10000))

    # add special partitions
    commands.append(insert_overwrite_partitioned_tbl_statement("TEST_TEXT_PARTITIONED_10x1K",
                                                               "mma_test",
                                                               123456,
                                                               1000))
    commands.append(insert_overwrite_partitioned_tbl_statement("TEST_ORC_PARTITIONED_10x1K",
                                                               "mma_test",
                                                               123456,
                                                               1000))
    commands.append(insert_overwrite_partitioned_tbl_statement("TEST_PARQUET_PARTITIONED_10x1K",
                                                               "mma_test",
                                                               123456,
                                                               1000))
    commands.append(insert_overwrite_partitioned_tbl_statement("TEST_RCFILE_PARTITIONED_10x1K",
                                                               "mma_test",
                                                               123456,
                                                               1000))
    commands.append(insert_overwrite_partitioned_tbl_statement(
        "TEST_SEQUENCEFILE_PARTITIONED_10x1K", "mma_test", 123456, 1000))
    commands.append(insert_overwrite_partitioned_tbl_statement("TEST_PARTITIONED_100x10K",
                                                               "mma_test",
                                                               123456,
                                                               10000))

    for c in setup_commands:
        utils.execute_command("hive -e \"%s\"" % c)

    executor = ThreadPoolExecutor(5)
    sql_2_future = {}
    for c in commands:
        sql_2_future[c] = executor.submit(utils.execute_command, "hive -e \"%s\"" % c)

    finished_sqls = set()

    progress_bar_length = 50
    spinning_cursors = ['|', '/', '-', '\\']
    spinning_cursor_idx = 0

    try:
        while True:
            if len(sql_2_future) == len(finished_sqls):
                break

            for sql in sql_2_future.keys():
                if sql_2_future[sql].done() and sql not in finished_sqls:
                    sql_2_future[sql].result()
                    finished_sqls.add(sql)

            progress = len(finished_sqls) / len(sql_2_future)
            hash_tag_length = int(progress * progress_bar_length)
            space_length = progress_bar_length - hash_tag_length
            spinning_cursor = spinning_cursors[spinning_cursor_idx % len(spinning_cursors)]
            progress_bar = '[%s%s] %s %s\r' % (hash_tag_length * '#',
                                               space_length * ' ',
                                               spinning_cursor,
                                               '%.2f%%' % float(progress * 100))
            spinning_cursor_idx += 1
            print(progress_bar, end='')
            time.sleep(0.5)

        print()
    except Exception as e:
        traceback.print_exc()
    finally:
        executor.shutdown()


if __name__ == '__main__':
    print("Initializing database 'MMA_TEST'")
    init_database()
    print("Done")

    print("Preparing data")
    prepare_data()
    print("Done")
