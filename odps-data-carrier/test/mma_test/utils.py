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
import subprocess
import traceback

from concurrent.futures import ThreadPoolExecutor


test_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
odps_data_carrier_dir = os.path.dirname(test_dir)


def execute_command(cmd):
    try:
        sp = subprocess.Popen(cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8')
        stdout, stderr = sp.communicate()
        if sp.returncode != 0:
            raise Exception(
                "Execute %s failed, stdout: %s, stderr %s\n" % (cmd, stdout, stderr))
        return stdout, stderr
    except Exception as e:
        raise Exception(traceback.format_exc())


def validate(hive_db,
             hive_tbl,
             mc_project,
             mc_tbl,
             hive_where_condition=None,
             mc_where_condition=None):

    def parse_hive_stdout(hive_stdout) -> float:
        return float(hive_stdout.strip())

    def parse_odps_stdout(odps_stdout) -> float:
        return float(odps_stdout.strip().split("\n")[1])

    odpscmd_path = os.path.join(odps_data_carrier_dir, "res", "console", "bin", "odpscmd")
    odps_config_path = os.path.join(odps_data_carrier_dir, "odps_config.ini")

    validate_sql = "select avg(t_smallint) from %s.%s %s;"

    executor = ThreadPoolExecutor(2)
    if hive_where_condition is None:
        hive_sql = validate_sql % (hive_db, hive_tbl, "")
    else:
        hive_sql = validate_sql % (hive_db, hive_tbl, "where " + hive_where_condition)
    hive_command = "hive -e '%s'" % hive_sql
    hive_future = executor.submit(execute_command, hive_command)

    if mc_where_condition is None:
        odps_sql = validate_sql % (mc_project, mc_tbl, "")
    else:
        odps_sql = validate_sql % (mc_project, mc_tbl, "where " + mc_where_condition)
    odps_sql = "set odps.sql.allow.fullscan=true; " + odps_sql
    odps_command = "%s --config=%s -M -e '%s'" % (odpscmd_path, odps_config_path, odps_sql)
    odps_future = executor.submit(execute_command, odps_command)

    hive_stdout, _ = hive_future.result()
    odps_stdout, _ = odps_future.result()

    return parse_hive_stdout(hive_stdout), parse_odps_stdout(odps_stdout)



