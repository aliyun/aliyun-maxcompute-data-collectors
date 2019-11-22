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

import logging
import os
import subprocess
import traceback
import threading
import time


class ProcessPool:

    _LOGGER = logging.getLogger("proccess pool")
    _PROCESS_POOL_JOIN_TIMEOUT = 0.01
    _PROCESS_POLL_ROLLING_INTERVAL = 1

    def __init__(self, capability: int, verbose: bool):
        self._capability = capability
        self._waiting_queue = []
        self._waiting_queue_lock = threading.Lock()
        self._proc_pool = []
        self._proc_pool_lock = threading.Lock()
        self._stopped = False
        self._stopped_lock = threading.Lock()
        self._consumer_thread = threading.Thread(target=self._consume)
        self._rolling_thread = threading.Thread(target=self._rolling)
        self._verbose = verbose
        if verbose:
            self._LOGGER.setLevel(logging.DEBUG)

    def start(self):
        self._consumer_thread.start()
        self._rolling_thread.start()

    def submit(self, command: str, log_dir: str, context: dict, retry=5) -> None:
        os.makedirs(log_dir, exist_ok=True)
        t = threading.Thread(
            target=self._execute,
            args=(command, log_dir, retry, context))
        with self._waiting_queue_lock:
            self._waiting_queue.append(t)

    def join_all(self):
        while True:
            with self._stopped_lock:
                if self._stopped:
                    break

            with self._proc_pool_lock:
                with self._waiting_queue_lock:
                    if len(self._proc_pool) == 0 and len(self._waiting_queue) == 0:
                        break

    def stop(self):
        with self._stopped_lock:
            self._stopped = True

    def _consume(self):
        while True:
            with self._stopped_lock:
                if self._stopped:
                    break

            with self._proc_pool_lock:
                with self._waiting_queue_lock:
                    if len(self._proc_pool) < self._capability and len(self._waiting_queue) > 0:
                        t = self._waiting_queue.pop(0)
                        t.start()
                        self._proc_pool.append(t)
            time.sleep(self._PROCESS_POLL_ROLLING_INTERVAL)

    def _rolling(self):
        while True:
            with self._stopped_lock:
                if self._stopped:
                    break

            with self._proc_pool_lock:
                for t in self._proc_pool:
                    t.join(timeout=self._PROCESS_POOL_JOIN_TIMEOUT)
                    if not t.is_alive():
                        self._LOGGER.info("thread finished")
                        self._proc_pool.remove(t)
            time.sleep(self._PROCESS_POLL_ROLLING_INTERVAL)

    def _execute(
            self,
            cmd: str,
            log_dir: str,
            retry: int,
            context: dict,
    ) -> None:
        self._LOGGER.info("execute \'%s\'" % cmd)
        num_retry_times = retry
        while retry > 0:
            try:
                if "on_submit_callback" in context:
                    context["on_submit_callback"](context)

                sp = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,
                    preexec_fn=os.setsid,
                    encoding='utf-8')
                stdout, stderr = sp.communicate()

                if self._verbose:
                    print("cmd: ")
                    print(cmd)
                    print("stdout: ")
                    print(stdout)
                    print("stderr: ")
                    print(stderr)

                if sp.returncode == 0:
                    stdout_path = os.path.join(log_dir, "stdout.log")
                    with open(stdout_path, 'a') as fd:
                        fd.write("=============================================================\n")
                        fd.write("cmd:\n")
                        fd.write(cmd + "\n")
                        fd.write("-------------------------------------------------------------\n")
                        fd.write(stdout + "\n")
                        fd.write("=============================================================\n")

                    stderr_path = os.path.join(log_dir, "stderr.log")
                    with open(stderr_path, 'a') as fd:
                        fd.write("=============================================================\n")
                        fd.write("cmd:\n")
                        fd.write(cmd + "\n")
                        fd.write("-------------------------------------------------------------\n")
                        fd.write(stderr + "\n")
                        fd.write("=============================================================\n")

                    if "on_success_callback" in context:
                        context["on_success_callback"](context)

                    break
                else:
                    log_path = os.path.join(log_dir, "error.log." + str(num_retry_times - retry))
                    with open(log_path, 'a') as fd:
                        fd.write("=============================================================\n")
                        fd.write("cmd:\n")
                        fd.write(cmd + "\n")
                        fd.write("-------------------------------------------------------------\n")
                        fd.write("stdout:\n")
                        fd.write(stdout + "\n")
                        fd.write("-------------------------------------------------------------\n")
                        fd.write("stderr:\n")
                        fd.write(stderr + "\n")
                        fd.write("=============================================================\n")

                retry -= 1
            except Exception as e:
                log_path = os.path.join(log_dir, "error.log." + str(num_retry_times - retry))
                with open(log_path, 'a') as fd:
                    fd.write("error:\n")
                    fd.write(traceback.format_exc())
                retry -= 1

        if retry == 0:
            self._LOGGER.error("execute \'%s\' failed %d times" % (cmd, num_retry_times))

