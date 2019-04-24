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

def main(root: str, odpscmd_path: str) -> None:
    databases = os.listdir(root)

    for database in databases:
        create_table_stmt_dir = os.path.join(
            root, database, "odps_ddl", "tables")
        add_partition_stmt_dir = os.path.join(
            root, database, "odps_ddl", "partitions")

    create_table_stmt_files = os.listdir(create_table_stmt_dir)
    add_partition_stmt_files = os.listdir(add_partition_stmt_dir)

    for create_table_stmt_file in create_table_stmt_files:
        file_path = os.path.join(create_table_stmt_dir, create_table_stmt_file)
        retry = 5
        while retry > 0:
            returncode = execute(
                "%s -f %s" % (odpscmd_path, file_path), verbose=True)
            if returncode == 0:
                break
            else:
                print("INFO: execute %s" % file_path + " failed, retrying...")
            retry -= 1
        if retry == 0:
            print("ERROR: execute %s" % file_path + " failed 5 times") 
        

    for add_partition_stmt_file in add_partition_stmt_files:
        file_path = os.path.join(
            add_partition_stmt_dir, add_partition_stmt_file)
        retry = 5
        returncode = execute(
            "%s -f %s" % (odpscmd_path, file_path), verbose=True)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('''
            usage: 
            python3 odps_ddl_runner.py <path to odps ddl> <path to odpscmd>
        ''')
        sys.exit(1)

    root, odpscmd_path = sys.argv[1], sys.argv[2]
    main(root, odpscmd_path)