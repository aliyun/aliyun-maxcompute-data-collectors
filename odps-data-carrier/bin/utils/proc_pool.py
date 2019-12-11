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
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import Future

from utils import print_utils


class ProcessPool:
    def __init__(self, capability: int, verbose: bool):
        self._capability = capability
        self._verbose = verbose
        self._pool = ThreadPoolExecutor(capability)

    def _process_and_log(self,
                         it: iter,
                         log_path: str,
                         buffer: list,
                         is_stdout: bool,
                         context: dict):
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, 'a') as fd:
            for line in it:
                buffer.append(line)
                if is_stdout and "on_stdout_output_callback" in context:
                    context["on_stdout_output_callback"](line, context)
                if not is_stdout and "on_stderr_output_callback" in context:
                    context["on_stderr_output_callback"](line, context)
                if self._verbose:
                    msg = "[Process pool] %s: %s" % ("STDOUT" if is_stdout else "STDERR", line)
                    print_utils.print_yellow(msg)
                fd.write(line)
                fd.flush()

    def _execute(self, cmd: str, log_dir: str, context: dict):
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
        stdout_lines = []
        stderr_lines = []
        stdout_reader = threading.Thread(target=self._process_and_log,
                                         args=(iter(sp.stdout.readline, ""),
                                               stdout_path,
                                               stdout_lines,
                                               True,
                                               context))
        stderr_reader = threading.Thread(target=self._process_and_log,
                                         args=(iter(sp.stderr.readline, ""),
                                               stderr_path,
                                               stderr_lines,
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
        else:
            raise Exception("Execution failed: " + cmd)

        return "".join(stdout_lines), "".join(stderr_lines)

    def submit(self, command: str, log_dir: str, context: dict) -> Future:
        return self._pool.submit(self._execute, command, log_dir, context)

    def shutdown(self):
        self._pool.shutdown()
