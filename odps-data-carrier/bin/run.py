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
import sys
import subprocess
import traceback
import time
import shutil
import threading

from hive_sql_runner import HiveSQLRunner
from odps_sql_runner import OdpsSQLRunner
from utils import print_utils


def execute_command(cmd):
    try:
        sp = subprocess.Popen(cmd, shell=True)
        sp.wait()
        if sp.returncode != 0:
            print_utils.print_red("Execute %s failed, exiting\n" % cmd)
    except Exception as e:
        print_utils.print_red(traceback.format_exc())
        sys.exit(1)


def parse_table_mapping(table_mapping_path):
    def parse_line(line: str):
        try:
            colon_idx = line.index(":")
        except ValueError as e:
            raise Exception("Cannot parse line: " + line)

        hive, mc = line[: colon_idx], line[colon_idx + 1:]

        try:
            dot_idx = hive.index(".")
        except ValueError as e:
            raise Exception("Cannot parse line: " + line)
        hive_db, hive_tbl = hive[: dot_idx].strip(), hive[dot_idx + 1:].strip()

        try:
            dot_idx = mc.index(".")
        except ValueError as e:
            raise Exception("Cannot parse line: " + line)
        mc_pjt, mc_tbl = mc[: dot_idx].strip(), mc[dot_idx + 1:].strip()

        return hive_db, hive_tbl, mc_pjt, mc_tbl

    table_mapping = {}
    db_mapping = {}
    with open(table_mapping_path, "r") as fd:
        for line in fd.readlines():
            hive_db, hive_tbl, mc_pjt, mc_tbl = parse_line(line)
            if (hive_db, hive_tbl) in table_mapping:
                raise Exception("Duplicated table mapping: " + line)
            if hive_db in db_mapping and db_mapping[hive_db] != mc_pjt:
                raise Exception("A Hive database is mapped to multiple MaxCompute project")
            table_mapping[(hive_db, hive_tbl)] = (mc_pjt, mc_tbl)
            db_mapping[hive_db] = mc_pjt
    return table_mapping


def validate_arguments(args):
    # validate arguments
    if args.hms_thrift_addr is None:
        print_utils.print_red("Must specify --hms_thrift_addr\n")
        sys.exit(1)

    if args.mode == "SINGLE":
        should_exit = False
        if args.hive_db is None:
            print_utils.print_red("Must specify --hive_db in SINGLE mode\n")
            should_exit = True
        if args.hive_table is None:
            print_utils.print_red("Must specify --hive_table in SINGLE mode\n")
            should_exit = True
        if args.mc_project is None:
            print_utils.print_red("Must specify --mc_project in SINGLE mode\n")
            should_exit = True
        if args.mc_table is None:
            print_utils.print_red("Must specify --mc_table in SINGLE mode\n")
            should_exit = True
        if should_exit:
            sys.exit(1)
    elif args.mode == "BATCH":
        should_exit = False
        if args.table_mapping is None:
            print_utils.print_red("Must specify --table_mapping in BATCH mode\n")
            should_exit = True
        if should_exit:
            sys.exit(1)
    else:
        print_utils.print_red("Invalid mode value, available values are SINGLE and BATCH\n")
        sys.exit(1)


def migrate(database,
            table,
            global_hive_sql_runner: HiveSQLRunner,
            odps_data_carrier_dir,
            script_root_dir):
    odps_log_dir = os.path.join(odps_data_carrier_dir, "log", "odps")
    hive_log_dir = os.path.join(odps_data_carrier_dir, "log", "hive")
    odps_ddl_dir = os.path.join(script_root_dir, "odps_ddl")
    hive_sql_dir = os.path.join(script_root_dir, "hive_udtf_sql")
    odps_sql_runner = OdpsSQLRunner(odps_data_carrier_dir, 30, False)
    # create table
    odps_sql_runner.execute(database,
                            table,
                            os.path.join(odps_ddl_dir, "create_table.sql"),
                            os.path.join(odps_log_dir, database, table),
                            True)
    odps_sql_runner.wait_for_completion()

    # create partitions
    scripts = os.listdir(odps_ddl_dir)
    for script in scripts:
        if "create_table.sql" == script:
            continue
        abs_script_path = os.path.join(odps_ddl_dir, script)
        odps_sql_runner.execute(database,
                                table,
                                abs_script_path,
                                os.path.join(odps_log_dir, database, table),
                                True)
    # wait until all
    odps_sql_runner.stop()
    # transfer data
    hive_sql_script_path = os.path.join(hive_sql_dir, "multi_partition", table + ".sql")
    global_hive_sql_runner.execute(database,
                                   table,
                                   hive_sql_script_path,
                                   os.path.join(hive_log_dir, database, table),
                                   True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run odps-data-carrier')
    parser.add_argument(
        "--hms_thrift_addr",
        required=True,
        type=str,
        help="Thrift address of Hive metastore.")
    parser.add_argument(
        "--mode",
        required=True,
        default="SINGLE",
        type=str,
        help="""Migration mode, SINGLE or BATCH.
        SINGLE means migrate one Hive table to MaxCompute, BATCH means migrate all the Hive tables 
        specified by the table mapping file""")
    parser.add_argument(
        "--hive_db",
        required=False,
        type=str,
        help="Specify Hive database in SINGLE mode")
    parser.add_argument(
        "--hive_table",
        required=False,
        type=str,
        help="Specify Hive table in SINGLE mode")
    parser.add_argument(
        "--mc_project",
        required=False,
        type=str,
        help="Specify MaxCompute project in SINGLE mode")
    parser.add_argument(
        "--mc_table",
        required=False,
        type=str,
        help="Specify MaxCompute table in SINGLE mode")
    parser.add_argument(
        "--parallelism",
        required=False,
        default=5,
        type=int,
        help="max parallelism of migration jobs")
    parser.add_argument(
        "--table_mapping",
        required=False,
        type=str,
        help="""The path of table mapping from Hive to MaxCompute in BATCH mode. Lines of the file 
        should be formatted as follows: <hive db>.<hive tbl>:<mc project>.<mc tbl>""")

    args = parser.parse_args()
    validate_arguments(args)

    bin_dir = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_dir = os.path.dirname(bin_dir)

    # run meta-carrier and replace MaxCompute project/table names
    print_utils.print_yellow("[Gathering metadata]\n")
    if args.mode == "SINGLE":
        meta_carrier_path = os.path.join(bin_dir, "meta-carrier")
        meta_carrier_output_dir = os.path.join(odps_data_carrier_dir,
                                               "meta_carrier_output_" + str(int(time.time())))
        execute_command("sh %s -u %s -t %s -o %s" % (meta_carrier_path,
                                                     args.hms_thrift_addr,
                                                     args.hive_db + "." + args.hive_table,
                                                     meta_carrier_output_dir))

        sed_mc_pjt_cmd = "sed -i 's#\"odpsProjectName\": .*,#\"odpsProjectName\": \"%s\",#g' %s"
        hive_db_config_path = os.path.join(meta_carrier_output_dir,
                                           args.hive_db,
                                           args.hive_db + ".json")
        execute_command(sed_mc_pjt_cmd % (args.mc_project, hive_db_config_path))

        sed_mc_tbl_cmd = "sed -i 's#\"odpsTableName\": .*,#\"odpsTableName\": \"%s\",#g' %s"
        hive_tbl_config_path = os.path.join(meta_carrier_output_dir,
                                            args.hive_db,
                                            "table_meta",
                                            args.hive_table + ".json")
        execute_command(sed_mc_tbl_cmd % (args.mc_table, hive_tbl_config_path))
    else:
        table_mapping = parse_table_mapping(args.table_mapping)
        tables = "\n".join(map(lambda key: key[0] + "." + key[1], table_mapping.keys()))
        meta_carrier_input_path = os.path.join(odps_data_carrier_dir,
                                               "meta-carrier_input_" + str(int(time.time())))
        with open(meta_carrier_input_path, "w") as fd:
            fd.write(tables)

        meta_carrier_path = os.path.join(bin_dir, "meta-carrier")
        meta_carrier_output_dir = os.path.join(odps_data_carrier_dir,
                                               "meta_carrier_output_" + str(int(time.time())))
        execute_command("sh %s -u %s -config %s -o %s" % (meta_carrier_path,
                                                          args.hms_thrift_addr,
                                                          meta_carrier_input_path,
                                                          meta_carrier_output_dir))
        os.unlink(meta_carrier_input_path)

        for hive_db, hive_tbl in table_mapping:
            mc_pjt, mc_tbl = table_mapping[(hive_db, hive_tbl)]
            sed_mc_pjt_cmd = "sed -i 's#\"odpsProjectName\": .*,#\"odpsProjectName\": \"%s\",#g' %s"
            hive_db_config_path = os.path.join(meta_carrier_output_dir, hive_db, hive_db + ".json")
            execute_command(sed_mc_pjt_cmd % (mc_pjt, hive_db_config_path))

            sed_mc_tbl_cmd = "sed -i 's#\"odpsTableName\": .*,#\"odpsTableName\": \"%s\",#g' %s"
            hive_tbl_config_path = os.path.join(meta_carrier_output_dir,
                                                hive_db,
                                                "table_meta",
                                                hive_tbl + ".json")
            execute_command(sed_mc_tbl_cmd % (mc_tbl, hive_tbl_config_path))
    print_utils.print_green("[Gathering metadata Done]\n")

    # run meta-processor
    print_utils.print_yellow("[Processing metadata]\n")
    meta_processor_path = os.path.join(bin_dir, "meta-processor")
    meta_processor_output_path = os.path.join(odps_data_carrier_dir,
                                              "meta_processor_output_" + str(int(time.time())))
    execute_command("sh %s -i %s -o %s" % (meta_processor_path,
                                           meta_carrier_output_dir,
                                           meta_processor_output_path))
    print_utils.print_green("[Processing metadata done]\n")

    # transfer data
    print_utils.print_yellow("[Migration starts]\n")
    global_hive_sql_runner = HiveSQLRunner(odps_data_carrier_dir, args.parallelism, True)
    migration_jobs = []
    databases = os.listdir(meta_processor_output_path)
    for database in databases:
        abs_database_dir = os.path.join(meta_processor_output_path, database)
        # skip report.html
        if not os.path.isdir(os.path.join(meta_processor_output_path, database)):
            continue

        tables = os.listdir(abs_database_dir)
        for table in tables:
            abs_table_dir = os.path.join(abs_database_dir, table)
            t = threading.Thread(target=migrate, args=(database,
                                                       table,
                                                       global_hive_sql_runner,
                                                       odps_data_carrier_dir,
                                                       abs_table_dir))
            t.start()
            migration_jobs.append((database, table, t))

    global_hive_sql_runner.stop()
    print_utils.print_green("[Migration done]\n")

    # clean up
    print_utils.print_yellow("[Cleaning]\n")
    shutil.rmtree(meta_carrier_output_dir)
    shutil.rmtree(meta_processor_output_path)
    print_utils.print_green("[Cleaning done]\n")
