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
import unittest
import mma_test.utils as utils

from mma_test.test_regular_migration import TestRegularMigration


def setup():
    tmp_path = os.path.join(utils.test_dir, "tmp")
    os.makedirs(tmp_path, exist_ok=True)
    # setup test env
    os.chdir(tmp_path)
    # generate configs
    utils.generate_mma_server_config()
    utils.generate_mma_client_config()


def regular_migration_test_suite():
    return unittest.defaultTestLoader.loadTestsFromTestCase(TestRegularMigration)


if __name__ == '__main__':
    suites = {"test_regular_migration": regular_migration_test_suite()}

    parser = argparse.ArgumentParser(description='Run odps-data-carrier tests')
    parser.add_argument(
        "--list_test_suites",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="list available test suites")
    parser.add_argument(
        "--list_test_cases",
        required=False,
        type=str,
        help="list available test cases for specified test suite")
    parser.add_argument(
        "--run_test_suite",
        required=False,
        help="run specified test suite")
    parser.add_argument(
        "--run_test_case",
        required=False,
        help="run specified test case, should be in format test_suite.test_case")
    parser.add_argument(
        "--fail_fast",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="fail fast")

    args = parser.parse_args()

    if args.list_test_suites:
        for suite in suites.keys():
            print(suite)
        exit(0)

    if args.list_test_cases is not None:
        if args.list_test_cases in suites:
            suite = suites[args.list_test_cases]
            for test in suite._tests:
                print(test.id().split(".")[-1])
            exit(0)
        else:
            raise Exception("Invalid test suite")

    if args.run_test_suite is not None and args.run_test_case is not None:
        raise Exception("--run_test_suite and --run_test_case cannot present at the same time")

    setup()

    # TODO: start mma-server if not exists automatically
    try:
        s = unittest.TestSuite()
        if args.run_test_suite is not None:
            if args.run_test_suite in suites:
                s.addTest(suites[args.run_test_suite])
            else:
                raise Exception("Invalid test suite")
        elif args.run_test_case is not None:
            splits = args.run_test_case.split(".")
            if len(splits) != 2:
                raise Exception("Invalid testcase: %s" % args.run_test_case)
            for test in suites[splits[0]]._tests:
                if splits[1] == test.id().split(".")[-1]:
                    s.addTest(test)
        else:
            s.addTests(suites.values())

        runner = unittest.TextTestRunner(verbosity=3, failfast=args.fail_fast, buffer=True)
        runner.run(s)
    finally:
        # TODO: kill mma-server
        pass


