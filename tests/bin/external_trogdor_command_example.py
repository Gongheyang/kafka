#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
import time

#
# This is an example of an external script which can be run through Trogdor's
# ExternalCommandWorker.
#

if __name__ == '__main__':
    # Read the ExternalCommandWorker start message.
    line = sys.stdin.readline()
    start_message = json.loads(line)
    workload = start_message["workload"]
    print("Starting external_trogdor_command_example with task id %s, workload %s" \
        % (start_message["id"], workload))
    sys.stdout.flush()
    print("{\"status\": \"running\"}")
    sys.stdout.flush()
    time.sleep(0.001 * workload["delayMs"])
    print("{\"status\": \"exiting after %s ms\"}" % workload["delayMs"])
    sys.stdout.flush()
