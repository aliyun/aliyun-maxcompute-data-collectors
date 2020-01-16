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

from odps_sql_runner import OdpsSQLRunner
from hive_sql_runner import HiveSQLRunner


def parse_table(table: str):
    try:
        dot_idx = table.index(".")
    except ValueError as e:
        raise Exception("Invalid input")

    return table[: dot_idx], table[dot_idx + 1:]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Partition diff tool')
    parser.add_argument(
        "--hive_table",
        required=True,
        type=str,
        help="Hive table, like <db>.<table>")
    parser.add_argument(
        "--mc_table",
        required=True,
        type=str,
        help="MC table, like <pjt>.<table>")
    parser.add_argument(
        "--verbose",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="print detailed information")
    args = parser.parse_args()

    hive_db, hive_table = parse_table(args.hive_table)
    mc_pjt, mc_table = parse_table(args.mc_table)

    bin_dir = os.path.dirname(os.path.realpath(__file__))
    odps_data_carrier_dir = os.path.dirname(bin_dir)
    log_dir = os.path.join(odps_data_carrier_dir, "tmp", "partition_diff")

    odps_sql_runner = OdpsSQLRunner(odps_data_carrier_dir, 1, False)
    hive_sql_runner = HiveSQLRunner(odps_data_carrier_dir, 1, False)

    try:
        f1 = odps_sql_runner.execute(mc_pjt, mc_table, "show partitions %s.%s" % (mc_pjt, mc_table), log_dir, False)
        f2 = hive_sql_runner.execute(hive_db, hive_table, "show partitions %s.%s" % (hive_db, hive_table), log_dir, False)

        stdout1, stderr1 = f1.result()
        stdout2, stderr2 = f2.result()

        mc_partitions = set(map(lambda x: x[:-1].strip() if x.endswith("\n") else x.strip(), stdout1.split("\n")))
        hive_partitions = list(map(lambda x: x[:-1].strip() if x.endswith("\n") else x.strip(), stdout2.split("\n")))
        for hive_partition in hive_partitions:
            if hive_partition not in mc_partitions:
                print(hive_partition)
    finally:
        odps_sql_runner.stop()
        hive_sql_runner.stop()
