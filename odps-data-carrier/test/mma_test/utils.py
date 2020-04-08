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
import time
import traceback
import configparser

from concurrent.futures import ThreadPoolExecutor

test_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
mma_server_config_path = os.path.join(test_dir, "tmp", "mma_server_config.json")
mma_client_config_path = os.path.join(test_dir, "tmp", "mma_client_config.json")

odps_data_carrier_dir = os.path.dirname(test_dir)

conf_dir = os.path.join(odps_data_carrier_dir, "conf")
hive_config_path = os.path.join(conf_dir, "hive_config.ini")
odps_config_path = os.path.join(conf_dir, "odps_config.ini")

bin_dir = os.path.join(odps_data_carrier_dir, "bin")
generate_config_path = os.path.join(bin_dir, "generate-config")
mma_server_path = os.path.join(bin_dir, "mma-server")
mma_client_path = os.path.join(bin_dir, "mma-client")

odpscmd_path = os.path.join(odps_data_carrier_dir, "res", "console", "bin", "odpscmd")

parser = configparser.ConfigParser()
with open(odps_config_path) as fd:
    parser.read_string("[dummy section]\n" + fd.read())
odps_config = parser["dummy section"]


def execute_command(cmd):
    try:
        print("Executing: %s" % cmd)

        sp = subprocess.Popen(cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8')
        stdout, stderr = sp.communicate()

        print("stdout: %s" % stdout)
        print("stderr: %s" % stderr)

        if sp.returncode != 0:
            raise Exception(
                "Execute %s failed, stdout: %s, stderr %s\n" % (cmd, stdout, stderr))
        return stdout, stderr
    except Exception as e:
        raise Exception(traceback.format_exc())


def execute_command_non_blocking(cmd) -> subprocess.Popen:
    try:
        print("Executing: %s" % cmd)

        return subprocess.Popen(cmd,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                encoding='utf-8')
    except Exception as e:
        raise Exception(traceback.format_exc())


def start_mma_server() -> subprocess.Popen:
    cmd = "sh %s --config %s" % (mma_server_path, mma_server_config_path)
    return execute_command_non_blocking(cmd)


def generate_migration_config(mapping: dict) -> str:
    cur_time = int(time.time())
    table_mapping_path = os.path.join(test_dir, "tmp", "temp_table_mapping_%s.txt" % str(cur_time))

    with open(table_mapping_path, 'w') as fd:
        for key in mapping.keys():
            hive_db, hive_tbl = key
            mc_db, mc_tbl = mapping[key]
            fd.write("%s.%s:%s.%s" % (hive_db, hive_tbl, mc_db, mc_tbl))

    cmd = "sh %s --table_mapping %s -m -p %s_" % (generate_config_path,
                                                  table_mapping_path,
                                                  str(cur_time))
    execute_command(cmd)
    return os.path.join(test_dir, "tmp", "%s_mma_migration_config.json" % str(cur_time))


def generate_mma_server_config() -> None:
    cmd = "sh %s --hive_config %s --odps_config %s -s" % (generate_config_path,
                                                          hive_config_path,
                                                          odps_config_path)
    execute_command(cmd)


def generate_mma_client_config() -> None:
    cmd = "sh %s --hive_config %s -c" % (generate_config_path,
                                         hive_config_path)
    execute_command(cmd)


def migrate(hive_db, hive_tbl, mc_pjt, mc_tbl):
    migration_config_path = generate_migration_config(
        {(hive_db, hive_tbl): (mc_pjt, mc_tbl)})

    start_command = "sh %s --config %s --start %s" % (mma_client_path,
                                                      mma_client_config_path,
                                                      migration_config_path)
    wait_command = "sh %s --config %s --wait %s.%s" % (mma_client_path,
                                                       mma_client_config_path,
                                                       hive_db,
                                                       hive_tbl)
    _, _ = execute_command(start_command)
    _, _ = execute_command(wait_command)


def verify(hive_db,
           hive_tbl,
           mc_project,
           mc_tbl,
           hive_where_condition=None,
           mc_where_condition=None):

    def parse_hive_stdout(hive_stdout: str) -> float:
        return float(hive_stdout.strip())

    def parse_odps_stdout(odps_stdout: str) -> float:
        return float(odps_stdout.strip().split("\n")[1])

    executor = ThreadPoolExecutor(2)

    sql = "SELECT AVG(t_smallint) FROM %s.%s %s;"

    if hive_where_condition is None:
        hive_sql = sql % (hive_db, hive_tbl, "")
    else:
        hive_sql = sql % (hive_db, hive_tbl, "WHERE " + hive_where_condition)
    hive_command = "hive -e '%s'" % hive_sql

    if mc_where_condition is None:
        odps_sql = sql % (mc_project, mc_tbl, "")
    else:
        odps_sql = sql % (mc_project, mc_tbl, "WHERE " + mc_where_condition)
    odps_sql = "set odps.sql.allow.fullscan=true; " + odps_sql
    odps_command = "%s --config=%s -M -e '%s'" % (odpscmd_path, odps_config_path, odps_sql)

    hive_future = executor.submit(execute_command, hive_command)
    odps_future = executor.submit(execute_command, odps_command)
    hive_stdout, _ = hive_future.result()
    odps_stdout, _ = odps_future.result()

    return parse_hive_stdout(hive_stdout), parse_odps_stdout(odps_stdout)
