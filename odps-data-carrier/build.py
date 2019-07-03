import argparse
import os
import subprocess
import re
import shutil
import sys

def execute(cmd: str, verbose=False) -> int:
  try:
    if (verbose):
      print("INFO: executing \'%s\'" % (cmd))

    sp = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
    sp.wait()

    return sp.returncode
  except Exception as e:
    print("ERROR: execute \'%s\'' Failed: %s" % (cmd, e))
    print(traceback.format_exc())
    return 1

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description='odps-data-carrier builder')

  tools = ["meta-carrier", "meta-processor", "odps-ddl-runner",
           "hive-udtf-sql-runner", "network-measurement-tool",
           "sql-checker"]

  parser.add_argument(
      "--hive_version",
      required=True,
      help="hive-version")
  parser.add_argument(
      "--excluded_tools",
      required=False,
      help=("tools to be excluded from the package, available values are: \n\t"
            + "\n\t".join(tools)),
      nargs='+')
  args = parser.parse_args()

  # goto the directory where the script is
  script_path = os.path.dirname(os.path.realpath(__file__))
  os.chdir(script_path)

  # replace hive version
  with open("pom.xml", "r+") as fd:
    content = fd.read()
    content = re.sub(
        "<hive.version>.*</hive.version>",
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
  os.makedirs("odps-data-carrier/bin")
  os.makedirs("odps-data-carrier/libs")
  os.makedirs("odps-data-carrier/res")
  shutil.copyfile("odps_config.ini", "odps-data-carrier/odps_config.ini")
  shutil.copyfile("extra_settings.ini", "odps-data-carrier/extra_settings.ini")

  # add tools
  excluded = args.excluded_tools
  if excluded is None:
    excluded = []
  print(excluded)

  if "meta-carrier" not in excluded:
    jar_name = "meta-carrier-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile(
        "bin/meta-carrier",
        "odps-data-carrier/bin/meta-carrier")
    shutil.copyfile(
        "meta-carrier/target/" + jar_name,
        "odps-data-carrier/libs/" + jar_name)

  if "meta-processor" not in excluded:
    jar_name = "meta-processor-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile(
        "bin/meta-processor",
        "odps-data-carrier/bin/meta-processor")
    shutil.copyfile(
        "meta-processor/target/" + jar_name,
        "odps-data-carrier/libs/" + jar_name)
    shutil.copyfile(
        "resources/style.css",
        "odps-data-carrier/res/style.css")

  if "odps-ddl-runner" not in excluded:
    shutil.copyfile(
        "bin/odps_ddl_runner.py",
        "odps-data-carrier/bin/odps_ddl_runner.py")
    execute("cp -r resources/console odps-data-carrier/res/")

  if "hive-udtf-sql-runner" not in excluded:
    jar_name = "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile(
        "bin/hive_udtf_sql_runner.py",
        "odps-data-carrier/bin/hive_udtf_sql_runner.py")
    shutil.copyfile(
        "data-transfer-hive-udtf/target/" + jar_name,
        "odps-data-carrier/libs/" + jar_name)

  if "network-measurement-tool" not in excluded:
    jar_name ="network-measurement-tool-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile(
        "bin/network-measurement-tool",
        "odps-data-carrier/bin/network-measurement-tool")
    shutil.copyfile(
        "network-measurement-tool/target/" + jar_name,
        "odps-data-carrier/libs/" + jar_name)

  if "sql-checker" not in excluded:
    jar_name = "odps-sql-migration-tool-wrapper-1.0-SNAPSHOT.jar"
    shutil.copyfile(
        "bin/sql-checker",
        "odps-data-carrier/bin/sql-checker")
    shutil.copyfile(
        "odps-sql-migration-tool-wrapper/target/" + jar_name,
        "odps-data-carrier/libs/" + jar_name)

  execute("zip -r odps-data-carrier.zip odps-data-carrier")
  shutil.rmtree("odps-data-carrier")
