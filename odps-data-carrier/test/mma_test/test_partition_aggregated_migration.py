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

# import os
# import time
# import traceback
# import unittest
# import mma_test.utils as utils


# class TestPartitionAggregatedMigration(unittest.TestCase):
#
#     executable_path = os.path.join(utils.odps_data_carrier_dir, "bin", "run.py")
#
#     def test_whole_table_partitioned_batch_mode(self):
#         mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))
#         table_mapping_path = os.path.join(utils.odps_data_carrier_dir,
#                                           "tmp",
#                                           "whole_table_partitioned_batch_mode.txt")
#         with open(table_mapping_path, 'w') as fd:
#             fd.write("MMA_TEST.TEST_PARTITIONED_100x10K{numOfPartitions=30}:%s.%s\n" %
#                      (config.project, mc_table))
#
#         name = "test_partition_aggregation"
#         command = ("python3 %s "
#                    "--hms_thrift_addr %s "
#                    "--mode BATCH "
#                    "--table_mapping %s "
#                    "--num_of_partitions 1 "
#                    "--name %s ") % (self.executable_path,
#                                     config.hms_thrift_addr,
#                                     table_mapping_path,
#                                     name)
#         try:
#             _, _ = utils.execute_command(command)
#             hive_avg, odps_avg = utils.validate("MMA_TEST",
#                                                 "TEST_PARTITIONED_100x10K",
#                                                 config.project,
#                                                 mc_table)
#             self.assertEquals(hive_avg, odps_avg)
#             hive_udtf_script_dir = os.path.join(utils.odps_data_carrier_dir,
#                                                 "tmp",
#                                                 "meta_processor_output_test_partition_aggregation",
#                                                 "mma_test",
#                                                 "test_partitioned_100x10k",
#                                                 "hive_udtf_sql",
#                                                 "multi_partition")
#             # should have 4 scripts (101//30 + 1)
#             self.assertEquals(4, len(os.listdir(hive_udtf_script_dir)))
#         except Exception as e:
#             self.fail(traceback.format_exc())
