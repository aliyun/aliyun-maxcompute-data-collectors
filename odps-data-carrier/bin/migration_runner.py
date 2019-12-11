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
# import sqlite3
import shutil
import subprocess
import sys
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from enum import Enum

from odps_sql_runner import OdpsSQLRunner
from hive_sql_runner import HiveSQLRunner
from data_validator import DataValidator
from utils import print_utils


def _execute_command(cmd):
    try:
        sp = subprocess.Popen(cmd, shell=True)
        sp.wait()
        if sp.returncode != 0:
            print_utils.print_red("Execute %s failed, exiting\n" % cmd)
            sys.exit(1)
    except Exception as e:
        print_utils.print_red(traceback.format_exc())
        sys.exit(1)


class MigrationRunner:
    class JobStatus(Enum):
        INIT = 1
        BUILD_TABLE_DONE = 2
        TRANSFER_DATA_DONE = 3
        VALIDATE_DONE = 4

    def __init__(self, odps_data_carrier_dir, table_mapping, hms_thrift_addr, parallelism, verbose):
        self._odps_data_carrier_dir = odps_data_carrier_dir
        self._table_mapping = table_mapping
        self._hms_thrift_addr = hms_thrift_addr
        self._parallelism = parallelism
        self._verbose = verbose

        # dir and paths
        self._timestamp = str(int(time.time()))
        self._meta_carrier_path = os.path.join(self._odps_data_carrier_dir, "bin", "meta-carrier")
        self._meta_carrier_input_path = os.path.join(self._odps_data_carrier_dir,
                                                     "tmp",
                                                     "meta-carrier_input_" + self._timestamp)
        self._meta_carrier_output_dir = os.path.join(self._odps_data_carrier_dir,
                                                     "tmp",
                                                     "meta-carrier_output_" + self._timestamp)
        self._meta_processor_path = os.path.join(self._odps_data_carrier_dir,
                                                 "bin",
                                                 "meta-processor")
        self._meta_processor_output_dir = os.path.join(self._odps_data_carrier_dir,
                                                       "tmp",
                                                       "meta_processor_output_" + self._timestamp)
        self._odps_log_root_dir = os.path.join(self._odps_data_carrier_dir, "log", "odps")
        self._hive_log_root_dir = os.path.join(self._odps_data_carrier_dir, "log", "hive")

        # global executors
        self._global_hive_sql_runner = HiveSQLRunner(self._odps_data_carrier_dir,
                                                     self._parallelism,
                                                     self._verbose)
        self._data_validator = DataValidator(self._odps_data_carrier_dir,
                                             self._parallelism,
                                             self._verbose)

        # status tracking
        self._num_jobs = 0
        self._num_succeed_jobs = 0
        self._num_failed_jobs = 0
        self._conn = None

    def _gather_metadata(self):
        tables = "\n".join(map(lambda key: key[0] + "." + key[1], self._table_mapping.keys()))
        with open(self._meta_carrier_input_path, "w") as fd:
            fd.write(tables)

        _execute_command("sh %s -u %s -config %s -o %s" % (self._meta_carrier_path,
                                                           self._hms_thrift_addr,
                                                           self._meta_carrier_input_path,
                                                           self._meta_carrier_output_dir))
        os.unlink(self._meta_carrier_input_path)

        for hive_db, hive_tbl in self._table_mapping:
            odps_pjt, odps_tbl = self._table_mapping[(hive_db, hive_tbl)]
            sed_odps_pjt_cmd = "sed -i 's#\"odpsProjectName\": .*,#\"odpsProjectName\": \"%s\",#g' %s"
            hive_db_config_path = os.path.join(self._meta_carrier_output_dir,
                                               hive_db,
                                               hive_db + ".json")
            _execute_command(sed_odps_pjt_cmd % (odps_pjt, hive_db_config_path))

            sed_odps_tbl_cmd = "sed -i 's#\"odpsTableName\": .*,#\"odpsTableName\": \"%s\",#g' %s"
            hive_tbl_config_path = os.path.join(self._meta_carrier_output_dir,
                                                hive_db,
                                                "table_meta",
                                                hive_tbl + ".json")
            _execute_command(sed_odps_tbl_cmd % (odps_tbl, hive_tbl_config_path))

    def _process_metadata(self):
        _execute_command("sh %s -i %s -o %s" % (self._meta_processor_path,
                                                self._meta_carrier_output_dir,
                                                self._meta_processor_output_dir))

    def _build_table(self, hive_db, hive_tbl, odps_pjt, odps_tbl):
        odps_sql_runner = OdpsSQLRunner(self._odps_data_carrier_dir, 50, self._verbose)
        try:
            odps_ddl_dir = os.path.join(self._meta_processor_output_dir,
                                        hive_db,
                                        hive_tbl,
                                        "odps_ddl")
            odps_log_dir = os.path.join(self._odps_log_root_dir, hive_db, hive_tbl)
            # create table
            script_path = os.path.join(odps_ddl_dir, "create_table.sql")
            create_table_future = odps_sql_runner.execute_script(hive_db,
                                                                 hive_tbl,
                                                                 script_path,
                                                                 odps_log_dir,
                                                                 True)
            # wait for success
            create_table_future.result()

            # add partitions
            add_partition_futures = []
            scripts = os.listdir(odps_ddl_dir)
            for script in scripts:
                if "create_table.sql" == script:
                    continue
                script_path = os.path.join(odps_ddl_dir, script)
                add_partition_futures.append(odps_sql_runner.execute_script(hive_db,
                                                                            hive_tbl,
                                                                            script_path,
                                                                            odps_log_dir,
                                                                            True))
            # wait for success
            for future in add_partition_futures:
                future.result()

            # update status
            self._update_job_status(hive_db,
                                    hive_tbl,
                                    odps_pjt,
                                    odps_tbl,
                                    self.JobStatus.BUILD_TABLE_DONE)
        except Exception as e:
            raise e
        finally:
            odps_sql_runner.stop()

    def _transfer_data(self, hive_db, hive_tbl, odps_pjt, odps_tbl):
        try:
            hive_sql_dir = os.path.join(self._meta_processor_output_dir,
                                        hive_db,
                                        hive_tbl,
                                        "hive_udtf_sql")
            hive_sql_script_path = os.path.join(hive_sql_dir, "multi_partition", hive_tbl + ".sql")
            hive_log_dir = os.path.join(self._hive_log_root_dir, hive_db, hive_tbl)
            self._global_hive_sql_runner.execute_script(hive_db,
                                                        hive_tbl,
                                                        hive_sql_script_path,
                                                        hive_log_dir,
                                                        True).result()
            self._update_job_status(hive_db,
                                    hive_tbl,
                                    odps_pjt,
                                    odps_tbl,
                                    self.JobStatus.TRANSFER_DATA_DONE)
        except Exception as e:
            pass

    def _validate_data(self, hive_db, hive_tbl, odps_pjt, odps_tbl):
        try:
            if self._data_validator.verify(hive_db, hive_tbl, odps_pjt, odps_tbl):
                # self._update_job_status(hive_db,
                #                         hive_tbl,
                #                         odps_pjt,
                #                         odps_tbl,
                #                         self.JobStatus.VALIDATE_DONE)
                pass
            else:
                # self._update_job_status(hive_db,
                #                         hive_tbl,
                #                         odps_pjt,
                #                         odps_tbl,
                #                         self.JobStatus.INIT)
                raise Exception("Data validation failed")
        except Exception as e:
            raise e

    # status related
    # def _init_job_status_db(self, table_mapping):
    #     self._conn = sqlite3.connect(os.path.join(self._odps_data_carrier_dir, "job_status.db"))
    #
    #     c = self._conn.cursor()
    #     c.execute("SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='job_status';")
    #     if c.fetchone() == 0:
    #         c.execute("""CREATE TABLE job_status
    #                      (
    #                          hive_db string,
    #                          hive_tbl string,
    #                          odps_pjt string,
    #                          odps_tbl string,
    #                          status int,
    #                          CONSTRAINT pk PRIMARY KEY (hive_db, hive_tbl, odps_pjt, odps_tbl)
    #                      );""")
    #         for hive_db, hive_tbl in table_mapping:
    #             odps_pjt, odps_tbl = table_mapping
    #             c.execute("""INSERT INTO job_status VALUES
    #                          (%s, %s, %s, %s, %d)""" % (hive_db,
    #                                                     hive_tbl,
    #                                                     odps_pjt,
    #                                                     odps_tbl,
    #                                                     self.JobStatus.INIT.value))
    #     c.close()

    def _update_job_status(self, hive_db, hive_tbl, odps_pjt, odps_tbl, status: JobStatus):
        # c = self._conn.cursor()
        # c.execute("""UPDATE job_status
        #              SET status=%d
        #              WHERE
        #              hive_db=%s and hive_tbl=%s and odps_pjt=%s and odps_tbl=%s;""" % (status.value,
        #                                                                                hive_db,
        #                                                                                hive_tbl,
        #                                                                                odps_pjt,
        #                                                                                odps_tbl))
        # c.close()
        # if status == self.JobStatus.VALIDATE_DONE:
        #     with open(os.path.join(self._odps_data_carrier_dir, "succeed.txt"), 'a') as fd:
        #         fd.write("%s.%s:%s.%s" % (hive_db, hive_tbl, odps_pjt, odps_tbl))
        #         print_utils.print_green("[SUCCEED] %s.%s -> %s.%s\n" % (hive_db,
        #                                                                 hive_tbl,
        #                                                                 odps_pjt,
        #                                                                 odps_tbl))
        # elif status == self.JobStatus.INIT:
        #     with open(os.path.join(self._odps_data_carrier_dir, "failed.txt"), 'a') as fd:
        #         fd.write("%s.%s:%s.%s" % (hive_db, hive_tbl, odps_pjt, odps_tbl))
        #         print_utils.print_red("[FAILED] %s.%s -> %s.%s\n" % (hive_db,
        #                                                              hive_tbl,
        #                                                              odps_pjt,
        #                                                              odps_tbl))
        pass

    # def _get_job_status(self, hive_db, hive_tbl, odps_pjt, odps_tbl):
    #     c = self._conn.cursor()
    #     c.execute("""SELECT status
    #                  FROM job_status
    #                  WHERE
    #                  hive_db=%s and hive_tbl=%s and odps_pjt=%s and odps_tbl=%s;""" % (hive_db,
    #                                                                                    hive_tbl,
    #                                                                                    odps_pjt,
    #                                                                                    odps_tbl))
    #     c.close()
    def _migrate(self, hive_db, hive_tbl, odps_pjt, odps_tbl):
        self._build_table(hive_db, hive_tbl, odps_pjt, odps_tbl)
        self._transfer_data(hive_db, hive_tbl, odps_pjt, odps_tbl)
        self._validate_data(hive_db, hive_tbl, odps_pjt, odps_tbl)

    def run(self, ):
        # TODO: add scheduling module
        # TODO: use sqlite to track migration status, support resuming
        def handle_finished_jobs():
            num_succeed_job = 0
            num_failed_job = 0
            for job in jobs:
                hive_db, hive_tbl, odps_pjt, odps_tbl, future = job
                mc_pjt, mc_tbl = self._table_mapping[(hive_db, hive_tbl)]
                if future.done():
                    jobs.remove(job)
                    try:
                        future.result()
                        num_succeed_job += 1
                        print_utils.print_green("[SUCCEED] %s.%s -> %s.%s\n" % (hive_db,
                                                                                hive_tbl,
                                                                                mc_pjt,
                                                                                mc_tbl))
                        succeed_list_path = os.path.join(self._odps_data_carrier_dir, "succeed.txt")
                        with open(succeed_list_path, 'a') as fd:
                            fd.write("%s.%s:%s.%s\n" % (hive_db, hive_tbl, odps_pjt, odps_tbl))
                    except Exception as e:
                        num_failed_job += 1
                        print_utils.print_red("[FAILED] %s.%s -> %s.%s\n" % (hive_db,
                                                                             hive_tbl,
                                                                             mc_pjt,
                                                                             mc_tbl))
                        print_utils.print_red(traceback.format_exc())
                        failed_list_path = os.path.join(self._odps_data_carrier_dir, "failed.txt")
                        with open(failed_list_path, 'a') as fd:
                            fd.write("%s.%s:%s.%s\n" % (hive_db, hive_tbl, odps_pjt, odps_tbl))
            return num_succeed_job, num_failed_job

        self._num_jobs = len(self._table_mapping)

        print_utils.print_yellow("[Gathering metadata]\n")
        self._gather_metadata()
        print_utils.print_green("[Gathering metadata Done]\n")

        print_utils.print_yellow("[Processing metadata]\n")
        self._process_metadata()
        print_utils.print_green("[Processing metadata done]\n")

        print_utils.print_yellow("[Migration starts]\n")
        executor = ThreadPoolExecutor(self._parallelism)
        jobs = []
        progress_format = "[Progress][%%%.2f] running: %d, succeed: %d, failed: %d, total: %d\n"
        for hive_db, hive_tbl in self._table_mapping:
            odps_pjt, odps_tbl = self._table_mapping[(hive_db, hive_tbl)]

            # wait for available slot
            while len(jobs) > self._parallelism + 10:
                progress = ((self._num_succeed_jobs + self._num_failed_jobs) / self._num_jobs) * 100
                print_utils.print_yellow(progress_format % (progress,
                                                            len(jobs),
                                                            self._num_succeed_jobs,
                                                            self._num_failed_jobs,
                                                            self._num_jobs))
                succeed, failed = handle_finished_jobs()
                self._num_succeed_jobs += succeed
                self._num_failed_jobs += failed
                time.sleep(10)

            future = executor.submit(self._migrate, hive_db, hive_tbl, odps_pjt, odps_tbl)
            jobs.append((hive_db, hive_tbl, odps_pjt, odps_tbl, future))

        while len(jobs) > 0:
            progress = ((self._num_succeed_jobs + self._num_failed_jobs) / self._num_jobs) * 100
            print_utils.print_yellow(progress_format % (progress,
                                                        len(jobs),
                                                        self._num_succeed_jobs,
                                                        self._num_failed_jobs,
                                                        self._num_jobs))
            succeed, failed = handle_finished_jobs()
            self._num_succeed_jobs += succeed
            self._num_failed_jobs += failed
            time.sleep(10)
        print_utils.print_green("[Migration done]\n")

        print_utils.print_yellow("[Cleaning]\n")
        shutil.rmtree(self._meta_carrier_output_dir)
        shutil.rmtree(self._meta_processor_output_dir)
        print_utils.print_green("[Cleaning done]\n")

    def stop(self):
        self._global_hive_sql_runner.stop()
        self._data_validator.stop()
