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

import time
import traceback
import unittest
import mma_test.utils as utils


class TestRegularMigration(unittest.TestCase):

    def test_whole_table_non_partitioned(self):
        timestamp = int(time.time())
        hive_db = "MMA_TEST"
        hive_tbl = "TEST_NON_PARTITIONED_1x100K"
        mc_pjt = utils.odps_config["project_name"]
        mc_tbl = "%s_%s" % (hive_db, str(timestamp))

        try:
            utils.migrate(hive_db, hive_tbl, mc_pjt, mc_tbl)
            hive_avg, odps_avg = utils.verify(hive_db,
                                              hive_tbl,
                                              mc_pjt,
                                              mc_tbl)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_whole_table_partitioned(self):
        timestamp = int(time.time())
        hive_db = "MMA_TEST"
        hive_tbl = "TEST_PARTITIONED_100x10K"
        mc_pjt = utils.odps_config["project_name"]
        mc_tbl = "%s_%s" % (hive_db, str(timestamp))

        try:
            utils.migrate(hive_db, hive_tbl, mc_pjt, mc_tbl)
            hive_avg, odps_avg = utils.verify(hive_db,
                                              hive_tbl,
                                              mc_pjt,
                                              mc_tbl)
            self.assertEquals(hive_avg, odps_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

if __name__ == '__main__':
    unittest.main()
