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
import time
import traceback
import unittest
import mma_test.config as config
import mma_test.utils as utils


class TestRegularMigration(unittest.TestCase):
    executable_path = os.path.join(utils.odps_data_carrier_dir, "bin", "run.py")

    def test_whole_table_non_partitioned_single_mode(self):
        mc_table = "TEST_NON_PARTITIONED_1M_" + str(int(time.time()))
        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode SINGLE "
                   "--hive_db MMA_TEST "
                   "--hive_table TEST_NON_PARTITIONED_1M "
                   "--mc_project %s "
                   "--mc_table %s") % (self.executable_path,
                                       config.hms_thrift_addr,
                                       config.project,
                                       mc_table)
        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_NON_PARTITIONED_1M",
                                                config.project,
                                                mc_table)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_whole_table_partitioned_single_mode(self):
        mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))
        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode SINGLE "
                   "--hive_db MMA_TEST "
                   "--hive_table TEST_PARTITIONED_100x10K "
                   "--mc_project %s "
                   "--mc_table %s") % (self.executable_path,
                                       config.hms_thrift_addr,
                                       config.project,
                                       mc_table)
        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_PARTITIONED_100x10K",
                                                config.project,
                                                mc_table)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_whole_table_non_partitioned_batch_mode(self):
        mc_table = "TEST_NON_PARTITIONED_1M_" + str(int(time.time()))
        table_mapping_path = os.path.join(utils.odps_data_carrier_dir,
                                          "tmp",
                                          "whole_table_non_partitioned_batch_mode.txt")
        with open(table_mapping_path, 'w') as fd:
            fd.write("MMA_TEST.TEST_NON_PARTITIONED_1M:%s.%s\n" % (config.project,
                                                                   mc_table))

        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode BATCH "
                   "--table_mapping %s") % (self.executable_path,
                                            config.hms_thrift_addr,
                                            table_mapping_path)
        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_NON_PARTITIONED_1M",
                                                config.project,
                                                mc_table)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_whole_table_partitioned_batch_mode(self):
        mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))
        table_mapping_path = os.path.join(utils.odps_data_carrier_dir,
                                          "tmp",
                                          "whole_table_partitioned_batch_mode.txt")
        with open(table_mapping_path, 'w') as fd:
            fd.write("MMA_TEST.TEST_PARTITIONED_100x10K:%s.%s\n" % (config.project,
                                                                    mc_table))

        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode BATCH "
                   "--table_mapping %s") % (self.executable_path,
                                            config.hms_thrift_addr,
                                            table_mapping_path)
        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_PARTITIONED_100x10K",
                                                config.project,
                                                mc_table)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    # single partition migration requires batch mode
    def test_single_partition_include(self):
        mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))
        table_mapping_path = os.path.join(utils.odps_data_carrier_dir,
                                          "tmp",
                                          "test_single_partition_include.txt")
        with open(table_mapping_path, 'w') as fd:
            fd.write("MMA_TEST.TEST_PARTITIONED_100x10K(p1=mma_test,p2=123456):%s.%s\n" %
                     (config.project, mc_table))

        name = "test_single_partition_include"
        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode BATCH "
                   "--table_mapping %s "
                   "--name %s") % (self.executable_path,
                                 config.hms_thrift_addr,
                                 table_mapping_path,
                                 name)

        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_PARTITIONED_100x10K",
                                                config.project,
                                                mc_table,
                                                hive_where_condition="p1=\"mma_test\" and p2=123456")
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    @unittest.skip("Not supported yet")
    def test_single_partition_exclude(self):
        mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))

        table_mapping_path = os.path.join(utils.odps_data_carrier_dir,
                                          "tmp",
                                          "test_single_partition_include.txt")
        with open(table_mapping_path, 'w') as fd:
            fd.write("!MMA_TEST.TEST_PARTITIONED_100x10K(p1=mma_test,p2=123456):%s.%s\n" %
                     (config.project, mc_table))
            fd.write("MMA_TEST.TEST_PARTITIONED_100x10K:%s.%s\n" % (config.project, mc_table))

        command = ("python3 %s "
                   "--hms_thrift_addr %s "
                   "--mode BATCH "
                   "--table_mapping %s") % (self.executable_path,
                                            config.hms_thrift_addr,
                                            table_mapping_path)

        try:
            _, _ = utils.execute_command(command)
            hive_avg, odps_avg = utils.validate("MMA_TEST",
                                                "TEST_PARTITIONED_100x10K",
                                                config.project,
                                                mc_table)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())


if __name__ == '__main__':
    unittest.main()
