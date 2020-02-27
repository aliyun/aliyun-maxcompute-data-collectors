import argparse
import os
import subprocess
import re
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

    optional_tools = ["network-measurement-tool", "sql-checker"]

    parser.add_argument(
        "--hive_version",
        required=True,
        help="hive-version")
    parser.add_argument(
        "--excluded_tools",
        required=False,
        help=("tools to be excluded, available values are: " + " ".join(optional_tools)),
        nargs='*')
    args = parser.parse_args()

    odps_data_carrier_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(odps_data_carrier_dir)

    if os.path.isdir("odps-data-carrier"):
        shutil.rmtree("odps-data-carrier")
    if os.path.exists("odps-data-carrier.tar.gz"):
        os.unlink("odps-data-carrier.tar.gz")

    # replace hive version
    with open("pom.xml", "r+") as fd:
        content = fd.read()
        content = re.sub("<hive.version>.*</hive.version>",
                         "<hive.version>%s</hive.version>" % args.hive_version,
                         content)
        fd.seek(0)
        fd.write(content)
        fd.truncate()

    # build and make dirs
    ret = execute("mvn clean package")
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    os.makedirs("odps-data-carrier")
    os.makedirs("odps-data-carrier/libs")
    # task-scheduler
    jar_name = "task-scheduler-1.0-SNAPSHOT.jar"
    original_jar_path = "task-scheduler/target/" + jar_name

    shutil.copyfile("bin/migrate", "odps-data-carrier/migrate")
    shutil.copyfile("bin/generate-config", "odps-data-carrier/generate-config")
    shutil.copyfile("odps_config.ini", "odps-data-carrier/odps_config.ini")
    shutil.copyfile(original_jar_path, "odps-data-carrier/" + jar_name)

    # data-transfer-hive-udtf
    udtf_jar_name = "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile("data-transfer-hive-udtf/target/" + udtf_jar_name, "odps-data-carrier/libs/" + udtf_jar_name)

    # generate config.json
    ret = execute("java -cp %s com.aliyun.odps.datacarrier.taskscheduler.MetaConfigurationUtils" % original_jar_path)
    if ret != 0:
        print("Generate config.json failed, exit")
        sys.exit(1)

    execute("tar -zcpvf odps-data-carrier.tar.gz odps-data-carrier")
    shutil.rmtree("odps-data-carrier")
    print("Done")
