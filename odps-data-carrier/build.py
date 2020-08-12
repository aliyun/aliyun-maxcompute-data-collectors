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
import subprocess
import shutil
import sys
import traceback


def execute(cmd: str, verbose=False) -> int:
    try:
        if verbose:
            print("INFO: executing \'%s\'" % cmd)

        sp = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
        sp.wait()

        return sp.returncode
    except Exception as e:
        print("ERROR: execute \'%s\'' Failed: %s" % (cmd, e))
        print(traceback.format_exc())
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='odps-data-carrier builder')

    parser.add_argument(
        "--test",
        required=False,
        const=True,
        default=False,
        action='store_const',
        help="include files for test")
    args = parser.parse_args()

    is_test_package = args.test

    odps_data_carrier_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(odps_data_carrier_dir)

    # remove existing package
    if os.path.isdir("odps-data-carrier"):
        shutil.rmtree("odps-data-carrier")
    if os.path.exists("odps-data-carrier.tar.gz"):
        os.unlink("odps-data-carrier.tar.gz")

    # install dingtalk jars
    ret = execute("mvn install:install-file -Dfile=task-scheduler/resource/taobao-sdk-java-auto_1479188381469-20200701.jar -DgroupId=com.dingtalk -DartifactId=dingtalk-sdk -Dversion=1.0 -Dpackaging=jar")
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    # build and make dirs
    ret = execute("mvn clean package -DskipTests")
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    # mkdirs
    os.makedirs("odps-data-carrier")
    os.makedirs("odps-data-carrier/bin")
    os.makedirs("odps-data-carrier/lib")
    os.makedirs("odps-data-carrier/conf")
    os.makedirs("odps-data-carrier/res")

    # task-scheduler
    jar_name = "task-scheduler-1.0-SNAPSHOT.jar"
    original_jar_path = "task-scheduler/target/" + jar_name

    # bin
    shutil.copyfile("bin/mma-server", "odps-data-carrier/bin/mma-server")
    shutil.copyfile("bin/mma-client", "odps-data-carrier/bin/mma-client")
    shutil.copyfile("bin/generate-config", "odps-data-carrier/bin/generate-config")
    shutil.copyfile("bin/configure", "odps-data-carrier/bin/configure")
    shutil.copyfile("bin/quickstart", "odps-data-carrier/bin/quickstart")

    # lib
    task_scheduler_jar_name = "task-scheduler-1.0-SNAPSHOT.jar"
    shutil.copyfile("task-scheduler/target/" + jar_name, "odps-data-carrier/lib/" + jar_name)
    udtf_jar_name = "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile("data-transfer-hive-udtf/target/" + udtf_jar_name,
                    "odps-data-carrier/lib/" + udtf_jar_name)

    # conf
    shutil.copyfile("odps_config.ini.template", "odps-data-carrier/conf/odps_config.ini.template")
    shutil.copyfile("oss_config.ini.template", "odps-data-carrier/conf/oss_config.ini.template")
    shutil.copyfile("hive_config.ini.template", "odps-data-carrier/conf/hive_config.ini.template")
    shutil.copyfile("table_mapping.txt", "odps-data-carrier/conf/table_mapping.txt")

    # res
    shutil.copyfile("mma_server_log4j2.xml", "odps-data-carrier/res/mma_server_log4j2.xml")
    shutil.copyfile("mma_client_log4j2.xml", "odps-data-carrier/res/mma_client_log4j2.xml")
    if is_test_package:
        shutil.copytree("resources/console", "odps-data-carrier/res/console")

    # test
    if is_test_package:
        shutil.copytree("test", "odps-data-carrier/test")

    execute("tar -zcpvf odps-data-carrier.tar.gz odps-data-carrier")
    shutil.rmtree("odps-data-carrier")
    print("Done")
