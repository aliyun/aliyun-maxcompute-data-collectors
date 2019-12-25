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


import argparse
import copy
import os
import re
from concurrent.futures import Future

from utils import print_utils
from utils.proc_pool import ProcessPool


'''
      [output directory]
      |______Report.html
      |______[database name]
             |______[table name]
                    |______odps_ddl
                    |      |______create_table.sql
                    |      |______create_partition_[partition spec].sql
                    |      |______...
                    |______hive_udtf_sql
                           |______single_partition
                           |      |______[partition spec].sql
                           |      |______...
                           |______multi_partition
                                  |______[table name].sql
'''


class HiveSQLRunner:
    _temp_func_name_multi = "odps_data_dump_multi"
    _class_name_multi = "com.aliyun.odps.datacarrier.transfer.OdpsDataTransferUDTF"
    _temp_func_name_single = "odps_data_dump_single"
    _class_name_single = "com.aliyun.odps.datacarrier.transfer.OdpsPartitionTransferUDTF"
    _udtf_jar_name = "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"

    def __init__(self, odps_data_carrier_dir: str, parallelism, verbose):
        self._odps_data_carrier_dir = odps_data_carrier_dir
        self._odps_config_path = os.path.join(odps_data_carrier_dir, "odps_config.ini")

        # init hadoop & hive settings

        extra_settings_path = os.path.join(odps_data_carrier_dir, "extra_settings.ini")
        self._settings = []
        self._load_settings(extra_settings_path)
        self._udtf_jar_path = os.path.join(odps_data_carrier_dir, "libs", self._udtf_jar_name)
        self._verbose = verbose
        self._pool = ProcessPool(parallelism, verbose)

    def _load_settings(self, path: str):
        with open(path, "r") as fd:
            for line in fd.readlines():
                if line is None or line.startswith("#") or len(line.strip()) == 0:
                    continue
                line = line.strip()
                if line.endswith("\n"):
                    line = line[: -1]
                if not line.endswith(";"):
                    line += ";"
                self._settings.append("set " + line)

    def _get_runnable_hive_sql_from_file(self, sql_script_path: str, is_udtf_sql: bool) -> str:
        with open(sql_script_path, "r") as fd:
            hive_sql = fd.read()

        return self._get_runnable_hive_sql(hive_sql=hive_sql, is_udtf_sql=is_udtf_sql)

    def _get_runnable_hive_sql(self, hive_sql: str, is_udtf_sql) -> str:
        lines = copy.deepcopy(self._settings)
        hive_sql = hive_sql.replace("\n", " ")
        hive_sql = hive_sql.replace("`", "")
        if is_udtf_sql:
            lines.append("add jar %s;" % self._udtf_jar_path)
            lines.append("add file %s;" % self._odps_config_path)
            lines.append("create temporary function %s as '%s';" % (self._temp_func_name_multi,
                                                                    self._class_name_multi))
            lines.append("create temporary function %s as '%s';" % (self._temp_func_name_single,
                                                                    self._class_name_single))
        lines.append(hive_sql)
        return " ".join(lines)

    def execute_script(self,
                       database_name: str,
                       table_name: str,
                       sql_script_path: str,
                       log_dir: str,
                       is_udtf_sql: bool) -> Future:
        def on_submit_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] Hive " + ("UDTF " if is_udtf_sql else "") + "SQL submitted\n"
                print_utils.print_yellow(msg % (database_name, table_name))

        def on_success_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] Hive " + ("UDTF " if is_udtf_sql else "") + "SQL finished\n"
                print_utils.print_green(msg % (database_name, table_name))

        def on_stderr_output_callback(line: str, context:dict):
            m = re.search(r'Starting Job = (.*), Tracking URL = (.*)', line)
            if m is not None:
                msg = "[%s.%s] Job ID = %s, Tracking URL = %s\n"
                print_utils.print_yellow(msg % (database_name,
                                                table_name,
                                                m.group(1),
                                                m.group(2)))

        context = {"type": "hive",
                   "on_submit_callback": on_submit_callback,
                   "on_success_callback": on_success_callback,
                   "on_stderr_output_callback": on_stderr_output_callback}

        command = "hive -e \"%s\"" % self._get_runnable_hive_sql_from_file(sql_script_path,
                                                                           is_udtf_sql)
        return self._pool.submit(command=command, log_dir=log_dir, context=context)

    def execute(self,
                database_name: str,
                table_name: str,
                sql: str,
                log_dir: str,
                is_udtf_sql: bool) -> Future:
        def on_submit_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] Hive " + ("UDTF " if is_udtf_sql else "") + "SQL submitted\n"
                print_utils.print_yellow(msg % (database_name, table_name))

        def on_success_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] Hive " + ("UDTF " if is_udtf_sql else "") + "SQL finished\n"
                print_utils.print_green(msg % (database_name, table_name))

        def on_stderr_output_callback(line: str, context:dict):
            m = re.search(r'Starting Job = (.*), Tracking URL = (.*)', line)
            if m is not None:
                msg = "[%s.%s] Job ID = %s, Tracking URL = %s\n"
                print_utils.print_yellow(msg % (database_name,
                                                table_name,
                                                m.group(1),
                                                m.group(2)))

        context = {"type": "hive",
                   "on_submit_callback": on_submit_callback,
                   "on_success_callback": on_success_callback,
                   "on_stderr_output_callback": on_stderr_output_callback}

        command = "hive -e \"%s\"" % self._get_runnable_hive_sql(sql, is_udtf_sql)
        return self._pool.submit(command=command, log_dir=log_dir, context=context)

    def stop(self):
        self._pool.shutdown()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hive SQL Runner')
    parser.add_argument(
        "--scripts",
        required=False,
        nargs='+',
        help="path to SQL scripts")
    parser.add_argument(
        "--parallelism",
        required=False,
        default=10,
        type=int,
        help="max parallelism of running odps ddl")
    parser.add_argument(
        "--verbose",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="print detailed information")
    args = parser.parse_args()

    # init hive_sql_runner
    bin_dir = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_dir = os.path.dirname(bin_dir)
    log_dir = os.path.join(odps_data_carrier_dir, "log", "hive_sql", "default")
    hive_sql_runner = HiveSQLRunner(odps_data_carrier_dir, args.parallelism, args.verbose)

    for script in args.scripts:
        hive_sql_runner.execute("N/A", "N/A", os.path.abspath(script), log_dir, True)

    hive_sql_runner.stop()

