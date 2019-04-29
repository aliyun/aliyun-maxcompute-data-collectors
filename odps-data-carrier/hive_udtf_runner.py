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

    for database in databases:
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
              "create temporary function odps_data_dump_multi as 'com.aliyun.odps.datacarrier.transfer.OdpsDataTransferUDTF';" +
              hive_multi_partition_sql)
          execute("hive -e \"%s\"" % hive_multi_partition_sql, verbose=True)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('''
            usage: 
            python3 hive_udtf_runner.py <path to generated dir> <udtf resource path> <odps config path>
        ''')
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])