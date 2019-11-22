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
import argparse
import subprocess
import copy
import traceback

from proc_pool import ProcessPool

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

temp_func_name_multi = "odps_data_dump_multi"
class_name_multi = "com.aliyun.odps.datacarrier.transfer.OdpsDataTransferUDTF"
temp_func_name_single = "odps_data_dump_single"
class_name_single = "com.aliyun.odps.datacarrier.transfer.OdpsPartitionTransferUDTF"


def submit(cmd: str, log_dir: str, context: dict, retry=5) -> None:
    pool.submit(command=cmd, log_dir=log_dir, context=context, retry=retry)


def get_runnable_hive_udtf_sql(
        file_path: str,
        udtf_resource_path: str,
        odps_config_path: str,
        extra_settings: str,
) -> str:
    with open(file_path) as fd:
        hive_sql = fd.read()
    with open(extra_settings) as fd:
        settings = fd.readlines()

    hive_sql = hive_sql.replace("\n", " ")
    hive_sql = hive_sql.replace("`", "")

    hive_sql_list = []
    hive_sql_list.append("add jar %s;" % udtf_resource_path)
    hive_sql_list.append("add file %s;" % odps_config_path)
    hive_sql_list.append("create temporary function %s as '%s';" % (
        temp_func_name_multi, class_name_multi))
    hive_sql_list.append("create temporary function %s as '%s';" % (
        temp_func_name_single, class_name_single))
    for setting in settings:
        if not setting.startswith("#") and len(setting.strip()) != 0:
            hive_sql_list.append("set %s;" % setting)
    hive_sql_list.append(hive_sql)

    return " ".join(hive_sql_list)


def get_runnable_hive_verify_sql(
        file_path: str,
        extra_settings: str,
) -> str:
    with open(file_path) as fd:
        hive_sql = fd.read()
    with open(extra_settings) as fd:
        settings = fd.readlines()

    hive_sql = hive_sql.replace("\n", " ")
    hive_sql = hive_sql.replace("`", "")

    hive_sql_list = []
    for setting in settings:
        if not setting.startswith("#") and len(setting.strip()) != 0:
            hive_sql_list.append("set %s;" % setting)
    hive_sql_list.append(hive_sql)

    return " ".join(hive_sql_list)


def print_red(s):
    print('\033[31m' + s + '\033[0m')


def print_yellow(s):
    print('\033[33m' + s + '\033[0m')


def print_green(s):
    print('\033[32m' + s + '\033[0m')


def on_success(context: dict):
    try:
        on_success_internal(context)
    except Exception as e:
        print_red("Exception happened, verification failed, now stop verification...")
        traceback.print_exc()


def on_success_internal(context: dict):

    def verify(odpscmd_path, odps_config_path, odps_verify_sql, hive_verify_sql, partition=None):
        odps_verify_sql = odps_verify_sql.replace("`", "")
        odpscmd_sp = subprocess.Popen(
            "%s --config=%s -M -e \"%s\"" % (odpscmd_path, odps_config_path, odps_verify_sql),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            preexec_fn=os.setsid,
            encoding='utf-8')

        hive_verify_sql = hive_verify_sql.replace("`", "")
        hive_sp = subprocess.Popen(
            "hive -e \"%s\"" % hive_verify_sql,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            preexec_fn=os.setsid,
            encoding='utf-8')

        odpscmd_stdout, odpscmd_stderr = odpscmd_sp.communicate()
        if odpscmd_sp.returncode != 0:
            print("[DEBUG] odpscmd ret val: " + str(odpscmd_sp.returncode))
            print("[DEBUG] odpscmd stderr: " + str(odpscmd_stderr))
            raise RuntimeError()

        hive_stdout, hive_stderr = hive_sp.communicate()
        if hive_sp.returncode != 0:
            print("[DEBUG] hive ret val: " + str(hive_sp.returncode))
            print("[DEBUG] hive stderr: " + str(hive_stderr))
            raise RuntimeError()

        odps_record_num = int(odpscmd_stdout.strip().split("\n")[1])
        hive_record_num = int(hive_stdout.strip())

        if odps_record_num != hive_record_num:
            print("==============================================================================")
            print_red("Database: %s" % context["database"])
            print_red("Table: %s" % context["table"])
            if partition is not None:
                print_red("Partition: %s" % partition)
            print_red("MaxCompute: %d" % odps_record_num)
            print_red("Hive: %d" % hive_record_num)
        else:
            print_green("Verification passed, database: %s, table: %s, partition %s, %d records" % (
                context["database"], context["table"], str(partition), odps_record_num))

    print_yellow("Hive UDTF SQL finished, database: %s, table: %s" % (context["database"],
                                                                      context["table"]))
    root_dir = context["root_dir"]
    odps_config_path = os.path.join(root_dir, "odps_config.ini")
    extra_settings_path = os.path.join(root_dir, "extra_settings.ini")
    odpscmd_path = os.path.join(root_dir, "res", "console", "bin", "odpscmd")
    processor_output_dir = context["processor_output_dir"]

    hive_single_partition_verify_dir = os.path.join(processor_output_dir,
                                                    context["database"],
                                                    context["table"],
                                                    "hive_verify_sql",
                                                    "single_partition")
    odps_single_partition_verify_dir = os.path.join(processor_output_dir,
                                                    context["database"],
                                                    context["table"],
                                                    "odps_verify_sql",
                                                    "single_partition")
    hive_whole_table_verify_path = os.path.join(processor_output_dir,
                                                context["database"],
                                                context["table"],
                                                "hive_verify_sql",
                                                context["table"] + ".sql")
    odps_whole_table_verify_path = os.path.join(processor_output_dir,
                                                context["database"],
                                                context["table"],
                                                "odps_verify_sql",
                                                context["table"] + ".sql")

    if os.path.isdir(hive_single_partition_verify_dir):
        single_partition_verify_files = os.listdir(hive_single_partition_verify_dir)
    else:
        single_partition_verify_files = []
    if len(single_partition_verify_files) == 0:
        # Not partitioned table
        with open(hive_whole_table_verify_path) as fd:
            hive_verify_sql = fd.read()

        with open(odps_whole_table_verify_path) as fd:
            odps_verify_sql = fd.read()

        try:
            verify(odpscmd_path, odps_config_path, odps_verify_sql, hive_verify_sql)
        except Exception as e:
            print_red("Exception happened, verification failed")
    else:
        # Partitioned table
        for single_partition_verify_file in single_partition_verify_files:
            odps_single_partition_verify_path = os.path.join(odps_single_partition_verify_dir,
                                                             single_partition_verify_file)
            hive_single_partition_verify_path = os.path.join(hive_single_partition_verify_dir,
                                                             single_partition_verify_file)
            hive_verify_sql = get_runnable_hive_verify_sql(hive_single_partition_verify_path,
                                                           extra_settings_path)
            with open(odps_single_partition_verify_path) as fd:
                odps_verify_sql = fd.read()

            try:
                verify(odpscmd_path,
                       odps_config_path,
                       odps_verify_sql,
                       hive_verify_sql,
                       single_partition_verify_file[: -4])
            except Exception as e:
                print_red("Exception happened, verification failed")
                print_red(str(e))
                traceback.print_exc()


def on_submit(context: dict):
    msg = "Hive UDTF SQL submitted, database: %s, table: %s"
    print_yellow(msg % (context["database"], context["table"]))


def run_all(
        root: str,
        udtf_resource_path: str,
        odps_config_path: str,
        hive_sql_log_root: str,
        extra_settings: str,
        context: dict
) -> None:

    context["type"] = "hive"
    context["processor_output_dir"] = root
    # register on success callback
    context["on_success_callback"] = on_success
    # register on submit callback
    context["on_submit_callback"] = on_submit

    databases = os.listdir(root)
    for database in databases:
        database_dir = os.path.join(root, database)

        # skip report.html
        if not os.path.isdir(database_dir):
            continue

        tables = os.listdir(database_dir)
        for table in tables:
            # hive multi-partition UDTF has a bug, so when user specifies "input_all", still use
            # single-partition UDTF for partitioned table. And for non-partitioned table,
            # use multi-partition is fine
            hive_multi_partition_sql_dir = os.path.join(
                database_dir, table, "hive_udtf_sql", "multi_partition")
            hive_single_partition_sql_dir = os.path.join(
                database_dir, table, "hive_udtf_sql", "single_partition")

            if os.path.isdir(hive_single_partition_sql_dir):
                single_partition_sql_files = os.listdir(hive_single_partition_sql_dir)
            else:
                single_partition_sql_files = []

            hive_sql_log_dir = os.path.join(hive_sql_log_root, database, table)
            os.makedirs(hive_sql_log_dir, exist_ok=True)

            context["database"] = database
            context["table"] = table

            # partitioned table use single partition UDTF
            # if len(single_partition_sql_files) != 0:
            #     for sql_file in single_partition_sql_files:
            #         sql_file_path = os.path.join(hive_single_partition_sql_dir, sql_file)
            #         hive_sql = get_runnable_hive_sql(sql_file_path,
            #                                          udtf_resource_path,
            #                                          odps_config_path,
            #                                          extra_settings)
            #         command = "hive -e \"%s\"" % hive_sql
            #         context["partition"] = sql_file[: -4]
            #         submit(command, log_dir=hive_sql_log_dir, context=context)
            # else:

            sql_file_path = os.path.join(hive_multi_partition_sql_dir, table + ".sql")
            hive_sql = get_runnable_hive_udtf_sql(sql_file_path,
                                             udtf_resource_path,
                                             odps_config_path,
                                             extra_settings)
            command = "hive -e \"%s\"" % hive_sql
            submit(command, log_dir=hive_sql_log_dir, context=copy.deepcopy(context))


def run_single_file(
        hive_single_partition_sql_path: str,
        udtf_resource_path: str,
        odps_config_path: str,
        hive_sql_log_root: str,
        extra_settings: str,
        context:dict
) -> None:

    hive_single_partition_sql = get_runnable_hive_udtf_sql(
        hive_single_partition_sql_path,
        udtf_resource_path,
        odps_config_path,
        extra_settings)

    hive_sql_log_dir = os.path.join(hive_sql_log_root, hive_single_partition_sql_path[: -4])
    os.makedirs(hive_sql_log_dir, exist_ok=True)

    command = "hive -e \"%s\"" % hive_single_partition_sql
    submit(command, log_dir=hive_sql_log_dir, context=context)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run hive UDTF SQL automatically.')
    parser.add_argument(
        "--input_all",
        required=False,
        help="path to directory generated by meta processor")
    parser.add_argument(
        "--input_single_file",
        required=False,
        help="path to a single sql file")
    parser.add_argument(
        "--settings",
        required=False,
        help="path to extra settings to set before running a hive sql")
    parser.add_argument(
        "--parallelism",
        required=False,
        default=5,
        type=int,
        help="max parallelism of running odps ddl")
    parser.add_argument(
        "--verbose",
        required=False,
        default=False,
        type=bool,
        help="print detailed information")
    args = parser.parse_args()

    context = {}

    # Get path to udtf jar & odps config
    script_path = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_path = os.path.dirname(script_path)

    context["root_dir"] = odps_data_carrier_path

    odps_config_path = os.path.join(
        odps_data_carrier_path, "odps_config.ini")
    extra_settings_path = os.path.join(
        odps_data_carrier_path, "extra_settings.ini")
    if not os.path.exists(odps_config_path):
        print("ERROR: %s does not exist" % odps_config_path)
        sys.exit(1)

    if args.input_single_file is not None:
        args.input_single_file = os.path.abspath(args.input_single_file)
    if args.input_all is not None:
        args.input_all = os.path.abspath(args.input_all)
    if args.settings is None:
        args.settings = extra_settings_path

    os.chdir(odps_data_carrier_path)

    udtf_path = os.path.join(
        odps_data_carrier_path,
        "libs",
        "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar")
    hive_sql_log_root = os.path.join(
        odps_data_carrier_path,
        "log",
        "hive_sql")

    if not os.path.exists(udtf_path):
        print("ERROR: %s does not exist" % udtf_path)
        sys.exit(1)

    pool = ProcessPool(args.parallelism, args.verbose)
    pool.start()

    try:
        if args.input_single_file is not None:
            run_single_file(args.input_single_file, udtf_path, odps_config_path, hive_sql_log_root,
                            args.settings, context)
        elif args.input_all is not None:
            run_all(args.input_all, udtf_path, odps_config_path, hive_sql_log_root,
                    args.settings, context)
        else:
            print("ERROR: please specify --input_all or --input_single_file")
    finally:
        pool.join_all()
        pool.stop()
