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
    def __init__(self, odps_data_carrier_dir, parallelism, verbose):
        self._odps_sql_runner = OdpsSQLRunner(odps_data_carrier_dir, parallelism, verbose)
        self._hive_sql_runner = HiveSQLRunner(odps_data_carrier_dir, parallelism, verbose)
        self._verbose = verbose

    def _is_partitioned_table(self, odps_stdout: str):
        odps_stdout = odps_stdout.strip()
        lines = odps_stdout.split("\n")
        if len(lines[0].split(",")) == 1:
            return False
        return True

    def _parse_odps_result(self, result: str) -> int:
        return int(result.strip().split("\n")[1])

    def _parse_hive_result(self, result: str) -> int:
        return int(result.strip())

    def _parse_odps_partitioned_table_result(self, result: str):
        lines = result.strip().split("\n")
        partition_spec_to_record_count = {}

        # get odps partition columns
        odps_partition_columns = lines[0].strip().split(",")[: -1]
        odps_partition_columns = list(map(lambda x: x.strip("\"").strip("'").strip(),
                                          odps_partition_columns))

        for line in lines[1:]:
            if line is None or len(line) == 0:
                continue
            splits = line.strip().split(",")
            # get rid of quotation marks and white chars
            splits = list(map(lambda x: x.strip("\"").strip("'").strip(), splits))
            partition_spec = self._get_partition_spec(odps_partition_columns, splits[: -1])
            record_count = int(splits[-1])
            partition_spec_to_record_count[partition_spec] = record_count
        return partition_spec_to_record_count

    def _parse_hive_partitioned_table_result(self, result: str, hive_db, hive_tbl, log_dir):
        lines = result.strip().split("\n")
        partition_spec_to_record_count = {}

        # get hive partition columns
        hive_partition_columns = self._get_hive_partition_columns(hive_db, hive_tbl, log_dir)

        for line in lines:
            if line is None or len(line) == 0:
                continue
            splits = line.split()
            # get rid of quotation marks and white chars
            splits = list(map(lambda x: x.strip("\"").strip("'").strip(), splits))
            partition_spec = self._get_partition_spec(hive_partition_columns, splits[: -1])
            record_count = int(splits[-1])
            partition_spec_to_record_count[partition_spec] = record_count
        return partition_spec_to_record_count

    def _get_hive_partition_columns(self, hive_db, hive_tbl, log_dir):
        future = self._hive_sql_runner.execute(hive_db,
                                               hive_tbl,
                                               "DESC %s.%s;" % (hive_db, hive_tbl),
                                               log_dir,
                                               False)
        hive_stdout, _ = future.result()
        lines = hive_stdout.strip().split("\n")
        partition_columns = []
        is_partition_column = False
        for line in lines:
            if "Partition Information" in line:
                is_partition_column = True
            if line is None or len(line.strip()) == 0 or line.startswith("#"):
                continue
            if is_partition_column:
                partition_columns.append(line.split()[0].strip())
        return partition_columns

    def _get_partition_spec(self, partition_columns, values):
        entries = []
        for i in range(len(values)):
            entries.append("%s=%s" % (partition_columns[i], values[i]))
        return ",".join(entries)

    def _clear_hive_stdout(self, hive_stdout):
        lines = hive_stdout.split("\n")
        cleared_lines = []
        for line in lines:
            if "parquet" in line:
                continue
            cleared_lines.append(line)
        return "\n".join(cleared_lines)

    def verify(self,
               hive_db,
               hive_tbl,
               mc_pjt,
               mc_tbl,
               hive_verify_sql_path,
               odps_verify_sql_path,
               log_root_dir,
               validate_failed_partition_list_path):
        log_dir = os.path.join(log_root_dir, hive_db, hive_tbl)
        # TODO: support more validation methods

        odps_verify_sql_future = self._odps_sql_runner.execute_script(hive_db,
                                                                      hive_tbl,
                                                                      odps_verify_sql_path,
                                                                      os.path.join(log_dir, "odps"),
                                                                      False)

        hive_verify_future = self._hive_sql_runner.execute_script(hive_db,
                                                                  hive_tbl,
                                                                  hive_verify_sql_path,
                                                                  os.path.join(log_dir, "hive"),
                                                                  False)

        odps_stdout, _ = odps_verify_sql_future.result()
        hive_stdout, _ = hive_verify_future.result()
        hive_stdout = self._clear_hive_stdout(hive_stdout)

        if self._is_partitioned_table(odps_stdout):
            ret = True
            fd = open(validate_failed_partition_list_path, 'a')
            odps_partition_spec_to_record_count = self._parse_odps_partitioned_table_result(
                odps_stdout)
            hive_partition_spec_to_record_count = self._parse_hive_partitioned_table_result(
                hive_stdout, hive_db, hive_tbl, os.path.join(log_dir, "hive", "desc"))
            for hive_partition_spec in hive_partition_spec_to_record_count.keys():
                if hive_partition_spec not in odps_partition_spec_to_record_count:
                    ret = False
                    fd.write("%s.%s(%s):%s.%s\n" % (hive_db,
                                                    hive_tbl,
                                                    hive_partition_spec,
                                                    mc_pjt,
                                                    mc_tbl))
                    continue

                odps_record_count = odps_partition_spec_to_record_count[hive_partition_spec]
                hive_record_count = hive_partition_spec_to_record_count[hive_partition_spec]
                if hive_record_count != odps_record_count:
                    ret = False
                    fd.write("%s.%s(%s):%s.%s\n" % (hive_db,
                                                    hive_tbl,
                                                    hive_partition_spec,
                                                    mc_pjt,
                                                    mc_tbl))
            fd.close()
            return ret
        else:
            return self._parse_odps_result(odps_stdout) == self._parse_hive_result(hive_stdout)

    def _get_partition_values_to_count(self, result: str):
        lines = result.strip().split("\n")
        partition_values_to_count = {}

        for line in lines[1:]:
            if line is None or len(line) == 0:
                continue
            splits = line.strip().split(",")
            # get rid of quotation marks and white chars
            partition_values = list(map(lambda x: x.strip("\"").strip("'").strip(), splits[: -1]))
            record_count = int(splits[-1])
            partition_values_to_count["/".join(partition_values)] = record_count
        return partition_values_to_count

    def verify_with_hive_meta(self,
                              hive_db,
                              hive_tbl,
                              mc_pjt,
                              mc_tbl,
                              odps_verify_sql_path,
                              hive_meta_partitioned,
                              hive_meta_non_partitioned,
                              log_root_dir,
                              validate_failed_partition_list_path):
        if ("%s.%s" % (hive_db, hive_tbl) not in hive_meta_partitioned and
                "%s.%s" % (hive_db, hive_tbl) not in hive_meta_non_partitioned):
            raise Exception("Hive metadata doesn't contain %s.%s" % (hive_db, hive_tbl))

        log_dir = os.path.join(log_root_dir, hive_db, hive_tbl)
        odps_verify_sql_future = self._odps_sql_runner.execute_script(hive_db,
                                                                      hive_tbl,
                                                                      odps_verify_sql_path,
                                                                      os.path.join(log_dir, "odps"),
                                                                      False)
        odps_stdout, _ = odps_verify_sql_future.result()
        if self._is_partitioned_table(odps_stdout):
            partition_cols = self._get_hive_partition_columns(hive_db,
                                                              hive_tbl,
                                                              os.path.join(log_dir, "hive", "desc"))
            partition_values_to_count = self._get_partition_values_to_count(odps_stdout)
            ret = True
            fd = open(validate_failed_partition_list_path, 'a')
            for partition_values in partition_values_to_count.keys():
                count = partition_values_to_count[partition_values]
                if partition_values not in hive_meta_partitioned["%s.%s" % (hive_db, hive_tbl)]:
                    raise Exception("Hive meta doesn't contains %s.%s(%s)" % (hive_db,
                                                                              hive_tbl,
                                                                              partition_values))
                if hive_meta_partitioned["%s.%s" % (hive_db, hive_tbl)][partition_values] != count:
                    ret = False
                    fd.write("%s.%s(%s):%s.%s\n" % (hive_db,
                                                    hive_tbl,
                                                    self._get_partition_spec(partition_cols, partition_values.split("/")),
                                                    mc_pjt,
                                                    mc_tbl))
            fd.close()
            return ret
        else:
            count = hive_meta_non_partitioned["%s.%s" % (hive_db, hive_tbl)]
            return self._parse_odps_result(odps_stdout) == count

    def stop(self):
        self._hive_sql_runner.stop()
        self._odps_sql_runner.stop()
