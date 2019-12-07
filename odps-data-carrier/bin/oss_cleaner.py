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
import threading
import sys
import time

import oss2


def warning(s):
    sys.stderr.write('\033[31m' + s + "\n" + '\033[0m')


def rename_internal(bucket: oss2.Bucket, source_obj, target_obj):
    bucket.copy_object(bucket.bucket_name, source_obj, target_obj)
    bucket.delete_object(source_obj)


def rename(pool: list, bucket: oss2.Bucket, source, target):
    t = threading.Thread(target=rename_internal, args=(bucket, source, target))
    t.start()
    pool.append(t)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='OSS cleaner')
    parser.add_argument(
        "--access_id",
        required=True,
        help="OSS access ID")
    parser.add_argument(
        "--access_key",
        required=True,
        help="OSS access Key")
    parser.add_argument(
        "--oss_endpoint",
        required=True,
        help="OSS endpoint")
    parser.add_argument(
        "--bucket",
        required=True,
        help="OSS bucket")
    parser.add_argument(
        "--table",
        required=True,
        default='',
        help="Hive table name")
    parser.add_argument(
        "--action",
        required=True,
        help="Can be scan, clean, or restore")

    args = parser.parse_args()
    if args.action.lower() not in ["scan", "clean", "restore"]:
        warning("Action must be scan, clean, or restore")
        sys.exit(1)

    auth = oss2.Auth(args.access_id, args.access_key)
    bucket = oss2.Bucket(auth, args.oss_endpoint, args.bucket)
    table = args.table
    if table != '' and not table.endswith("/"):
        table += "/"

    if args.action.lower() == "clean" or args.action.lower() == "scan":
        staging_files = []
        pattern = "/.hive-staging"
        for b in oss2.ObjectIterator(bucket, prefix=table):
            if pattern in b.key:
                warning("Found staging file: " + b.key)
                staging_files.append(b.key)
        print("Scan finished, found " + str(len(staging_files)) + " staging files")

        if args.action.lower() == "clean":
            threads = []
            finished = 0
            print("Move staging files to directory /.Staging")
            for file in staging_files:
                rename(threads, bucket, file, ".Staging/" + file)

            while True:
                for t in threads:
                    if not t.is_alive():
                        finished += 1
                        threads.remove(t)
                if len(threads) != 0:
                    print("total: %d, finished: %d, running: %d" % (len(staging_files),
                                                                    finished,
                                                                    len(threads)))
                    time.sleep(10)
                else:
                    break
        print("Done")

    if args.action.lower() == "restore":
        staging_files = []
        for b in oss2.ObjectIterator(bucket, prefix=".Staging/" + args.table):
            warning("Found staging file: " + b.key)
            staging_files.append(b.key)
        print("Scan finished, found " + str(len(staging_files)) + " staging files")
        threads = []
        finished = 0
        print("Restore starts")
        for file in staging_files:
            rename(threads, bucket, file, file[len(".Staging/"):])

        while True:
            for t in threads:
                if not t.is_alive():
                    threads.remove(t)
                    finished += 1
            if len(threads) != 0:
                print("total: %d, finished: %d, running: %d" % (len(staging_files),
                                                                finished,
                                                                len(threads)))
                time.sleep(3)
            else:
                break
        print("Done")
        print("Scan finished, found " + str(len(staging_files)) + " staging files")

