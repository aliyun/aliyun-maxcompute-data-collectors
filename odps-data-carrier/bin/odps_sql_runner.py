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


class OdpsSQLRunner:
    def __init__(self, odps_data_carrier_dir: str, parallelism, verbose):
        self._odps_config_path = os.path.join(odps_data_carrier_dir, "odps_config.ini")
        self._odpscmd_path = os.path.join(odps_data_carrier_dir, "res", "console", "bin", "odpscmd")
        self._verbose = verbose
        self._pool = ProcessPool(parallelism, verbose)

    def execute_script(self,
                       database_name: str,
                       table_name: str,
                       sql_script_path: str,
                       log_dir: str,
                       is_ddl: bool) -> Future:
        def on_submit_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] ODPS " + ("DDL" if is_ddl else "SQL") + " submitted\n"
                print_utils.print_yellow(msg % (database_name, table_name))

        def on_success_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] ODPS " + ("DDL" if is_ddl else "SQL") + " finished\n"
                print_utils.print_green(msg % (database_name, table_name))

        def on_stderr_output_callback(line: str, context:dict):
            if self._verbose:
                m = re.search(r'ID = (.*)', line)
                if m is not None:
                    msg = "[%s.%s] Instance ID = %s\n"
                    print_utils.print_yellow(msg % (database_name, table_name, m.group(1)))
                m = re.search(r'http://logview.*', line)
                if m is not None:
                    msg = "[%s.%s] Log view = %s\n"
                    print_utils.print_yellow(msg % (database_name, table_name, m.group(0)))

        context = {"type": "odps",
                   "on_submit_callback": on_submit_callback,
                   "on_success_callback": on_success_callback,
                   "on_stderr_output_callback": on_stderr_output_callback}

        command = "%s --config=%s -M -f %s" % (self._odpscmd_path,
                                               self._odps_config_path,
                                               sql_script_path)
        return self._pool.submit(command=command, log_dir=log_dir, context=context)

    def execute(self,
                database_name: str,
                table_name: str,
                sql: str,
                log_dir: str,
                is_ddl: bool) -> Future:
        def on_submit_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] ODPS " + ("DDL" if is_ddl else "SQL") + " submitted\n"
                print_utils.print_yellow(msg % (database_name, table_name))

        def on_success_callback(context: dict):
            if self._verbose:
                msg = "[%s.%s] ODPS " + ("DDL" if is_ddl else "SQL") + " finished\n"
                print_utils.print_green(msg % (database_name, table_name))

        def on_stderr_output_callback(line: str, context:dict):
            m = re.search(r'ID = (.*)', line)
            if m is not None:
                msg = "[%s.%s] Instance ID = %s\n"
                print_utils.print_yellow(msg % (database_name, table_name, m.group(1)))

        context = {"type": "odps",
                   "on_submit_callback": on_submit_callback,
                   "on_success_callback": on_success_callback,
                   "on_stderr_output_callback": on_stderr_output_callback}

        sql = sql.replace("`", "").replace("\n", " ")
        command = "%s --config=%s -M -e \"%s\"" % (self._odpscmd_path,
                                                   self._odps_config_path,
                                                   sql)
        return self._pool.submit(command=command, log_dir=log_dir, context=context)

    def stop(self):
        self._pool.shutdown()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ODPS SQL Runner')
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

    bin_dir = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_dir = os.path.dirname(bin_dir)
    log_dir = os.path.join(odps_data_carrier_dir, "log", "odps_sql", "default")
    odps_sql_runner = OdpsSQLRunner(odps_data_carrier_dir, args.parallelism, args.verbose)

    for script in args.scripts:
        odps_sql_runner.execute("N/A", "N/A", os.path.abspath(script), log_dir, False)

    odps_sql_runner.stop()
