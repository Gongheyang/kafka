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

from datetime import date
import argparse
from distutils.dir_util import copy_tree
import os


def remove_args_and_hardcode_values(file_path, kafka_url):
    with open(file_path, 'r') as file:
        filedata = file.read()
    filedata = filedata.replace("ARG kafka_url", f"ENV kafka_url {kafka_url}")
    filedata = filedata.replace(
        "ARG build_date", f"ENV build_date {str(date.today())}")
    with open(file_path, 'w') as file:
        file.write(filedata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-type", "-type", choices=[
                        "jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-version", "-v", dest="kafka_version",
                        help="Kafka version for which the source for docker official image is to be built")
    args = parser.parse_args()
    
    kafka_url = f"https://downloads.apache.org/kafka/{args.kafka_version}/kafka_2.13-{args.kafka_version}.tgz"
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    new_dir = os.path.join(
        current_dir, f'docker_official_images', args.kafka_version)
    os.makedirs(new_dir, exist_ok=True)
    copy_tree(f"{current_dir}/jvm", os.path.join(new_dir, args.kafka_version, 'jvm'))
    copy_tree(f"{current_dir}/resources", os.path.join(new_dir, args.kafka_version, 'jvm/resources'))
    remove_args_and_hardcode_values(
        os.path.join(new_dir, args.kafka_version, 'jvm/Dockerfile'), kafka_url)
