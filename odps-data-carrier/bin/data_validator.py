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

from hive_sql_runner import HiveSQLRunner
from odps_sql_runner import OdpsSQLRunner


class DataValidator:
    def __init__(self,odps_data_carrier_dir, parallelism, verbose):
        self._log_dir = os.path.join(odps_data_carrier_dir, "log", "verify")
        self._odps_sql_runner = OdpsSQLRunner(odps_data_carrier_dir, parallelism, verbose)
        self._hive_sql_runner = HiveSQLRunner(odps_data_carrier_dir, parallelism, verbose)
        self._verbose = verbose

    def _parse_odps_result(self, result: str) -> int:
        return int(result.strip().split("\n")[1])

    def _parse_hive_result(self, result: str) -> int:
        return int(result.strip())

    def verify(self, hive_db, hive_tbl, mc_pjt, mc_tbl):
        log_dir = os.path.join(self._log_dir, hive_db, hive_tbl)
        # TODO: support more validation methods
        odps_settings = "SET odps.sql.allow.fullscan=true;"
        odps_verify_sql = "SELECT count(1) FROM %s.%s;" % (mc_pjt, mc_tbl)

        odps_verify_sql_future = self._odps_sql_runner.execute(hive_db,
                                                             hive_tbl,
                                                             odps_settings + odps_verify_sql,
                                                             os.path.join(log_dir, "odps"),
                                                             False)

        hive_verify_sql = "SELECT count(1) FROM %s.%s;" % (hive_db, hive_tbl)
        hive_verify_future = self._hive_sql_runner.execute(hive_db,
                                                           hive_tbl,
                                                           hive_verify_sql,
                                                           os.path.join(log_dir, "hive"),
                                                           False)

        odps_stdout, _ = odps_verify_sql_future.result()
        hive_stdout, _ = hive_verify_future.result()
        if self._parse_odps_result(odps_stdout) == self._parse_hive_result(hive_stdout):
            return True
        else:
            return False

    def stop(self):
        self._hive_sql_runner.stop()
        self._odps_sql_runner.stop()
