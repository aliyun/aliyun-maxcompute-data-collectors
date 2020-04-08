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


class TestValidation(unittest.TestCase):

    executable_path = os.path.join(utils.odps_data_carrier_dir, "bin", "run.py")

    def test_incorrect_data(self):
        # mc_table = "TEST_PARTITIONED_100x10K_" + str(int(time.time()))
        # command = ("python3 %s "
        #            "--hms_thrift_addr %s "
        #            "--mode SINGLE "
        #            "--hive_db MMA_TEST "
        #            "--hive_table TEST_PARTITIONED_100x10K "
        #            "--mc_project %s "
        #            "--mc_table %s") % (self.executable_path,
        #                                config.hms_thrift_addr,
        #                                config.project,
        #                                mc_table)
        # try:
        #     _, _ = utils.execute_command(command)
        #     hive_avg, odps_avg = utils.validate("MMA_TEST",
        #                                         "TEST_PARTITIONED_100x10K",
        #                                         config.project,
        #                                         mc_table)
        #     self.assertEquals(hive_avg, odps_avg)
        # except Exception as e:
        #     self.fail(traceback.format_exc())
        pass


    def test_additional_partition(self):
        pass

    def test_correct_data(self):
        pass
