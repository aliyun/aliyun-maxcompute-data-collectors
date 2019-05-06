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
import sys
import subprocess
import traceback

'''
   [output directory]
   |______Report.html
   |______[database name]
          |______odps_ddl
          |      |______tables
          |      |      |______[table name].sql
          |      |______partitions
          |             |______[table name].sql
          |______hive_udtf_sql
                 |______single_partition
                 |      |______[table name].sql
                 |______multi_partition
                        |______[table name].sql
'''

def execute(cmd: str, verbose=False) -> int:
    try:
        if (verbose):
            print("INFO: executing \'%s\'" %(cmd))

        sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, preexec_fn = os.setsid)
        sp.wait()

        if (verbose):
            stdout = sp.stdout.read().strip()
            stderr = sp.stderr.read().strip()
            print("DEBUG: stdout: " + str(stdout))
            print("DEBUG: stderr: " + str(stderr))
            print("DEBUG: returncode: " + str(sp.returncode))

        return sp.returncode
    except Exception as e:
        print("ERROR: execute \'%s\'' Failed: %s" %(cmd, e))
        print(traceback.format_exc())
        return 1

def main(root: str, udtf_resource_path: str, odps_config_path: str) -> None:
    databases = os.listdir(root)
    temp_func_name = "odps_data_dump_multi"
    class_name = "com.aliyun.odps.datacarrier.transfer.OdpsDataTransferUDTF"

    for database in databases:
        if database == "report.html":
            continue
        hive_multi_partition_sql_dir = os.path.join(
            root, database, "hive_udtf_sql", "multi_partition")

        hive_multi_partition_sql_files = os.listdir(
          hive_multi_partition_sql_dir)

        for hive_multi_partition_sql_file in hive_multi_partition_sql_files:
          file_path = os.path.join(
            hive_multi_partition_sql_dir, hive_multi_partition_sql_file)
          with open(file_path) as fd:
            hive_multi_partition_sql = fd.read()
            hive_multi_partition_sql = hive_multi_partition_sql.replace(
              "\n", " ")
            hive_multi_partition_sql = hive_multi_partition_sql.replace(
              "`", "")
            hive_multi_partition_sql = (
              "add jar %s;" % udtf_resource_path +
              "add file %s;" % odps_config_path +
              "create temporary function %s as '%s';" % (temp_func_name, class_name) +
              hive_multi_partition_sql)

            retry = 5
            while retry > 0:
              returncode = execute(
                  "hive -e \"%s\"" % hive_multi_partition_sql, verbose=True)
              if returncode == 0:
                break
              else:
                print("INFO: execute %s failed, retrying..." % file_path)
              retry -= 1

            if retry == 0:
              print("ERROR: execute %s failed 5 times" % file_path)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('''
            usage: 
            python3 hive_udtf_sql_runner.py <hive sql path>
        ''')
        sys.exit(1)

    # Get path to udtf jar & odps config
    hive_sql_path = os.path.abspath(sys.argv[1])
    script_path = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_path = os.path.dirname(script_path)
    os.chdir(odps_data_carrier_path)

    udtf_path = os.path.join(
        odps_data_carrier_path,
        "libs",
        "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    )
    if not os.path.exists(udtf_path):
      print("ERROR: %s does not exist" % udtf_path)
      sys.exit(1)

    odps_config_path = os.path.join(
        odps_data_carrier_path,
        "res",
        "console",
        "conf",
        "odps_config.ini"
    )
    if not os.path.exists(odps_config_path):
      print("ERROR: %s does not exist" % udtf_path)
      sys.exit(1)

    main(hive_sql_path, udtf_path, odps_config_path)