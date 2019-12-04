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
import subprocess
import traceback
import threading
import time

from utils import print_utils

class ProcessPool:

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

    def start(self):
        self._consumer_thread.start()
        self._rolling_thread.start()

    def submit(self, command: str, log_dir: str, context: dict, retry=5) -> None:
        os.makedirs(log_dir, exist_ok=True)
        t = threading.Thread(target=self._execute, args=(command, log_dir, retry, context))
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

    def status(self):
        with self._waiting_queue_lock:
            waiting = len(self._waiting_queue)
        with self._proc_pool_lock:
            running = len(self._proc_pool)
        return waiting, running

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
                        self._proc_pool.remove(t)
            time.sleep(self._PROCESS_POLL_ROLLING_INTERVAL)

    def _process_and_log(self, it: iter, log_path: str, is_stdout: bool, context: dict):
        with open(log_path, 'a') as fd:
            for line in it:
                if is_stdout and "on_stdout_output_callback" in context:
                    context["on_stdout_output_callback"](line, context)
                if not is_stdout and "on_stderr_output_callback" in context:
                    context["on_stderr_output_callback"](line, context)
                if self._verbose:
                    msg = "[Process pool] %s: %s" % ("STDOUT" if is_stdout else "STDERR", line)
                    print_utils.print_yellow(msg)
                fd.write(line)
                fd.flush()

    def _execute(self, cmd: str, log_dir: str, retry: int, context: dict) -> None:
        while retry >= 0:
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

                stdout_path = os.path.join(log_dir, "stdout.log")
                stderr_path = os.path.join(log_dir, "stderr.log")
                stdout_reader = threading.Thread(target=self._process_and_log,
                                                 args=(iter(sp.stdout.readline, ""),
                                                       stdout_path,
                                                       True,
                                                       context))
                stderr_reader = threading.Thread(target=self._process_and_log,
                                                 args=(iter(sp.stderr.readline, ""),
                                                       stderr_path,
                                                       False,
                                                       context))
                stdout_reader.start()
                stderr_reader.start()
                sp.wait()
                stdout_reader.join()
                stderr_reader.join()

                if sp.returncode == 0:
                    if "on_success_callback" in context:
                        context["on_success_callback"](context)
                    break
                else:
                    retry -= 1
            except Exception as e:
                log_path = os.path.join(log_dir, "error.log")
                with open(log_path, 'a') as fd:
                    fd.write("error:\n")
                    fd.write(traceback.format_exc())
                retry -= 1

        if retry < 0:
            print_utils.print_red("execute \'%s\' failed" % cmd)
