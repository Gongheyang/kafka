#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SCALA_VERSION="2.13"
VERSION="4.0.0-SNAPSHOT"

RELEASE_DIR="kafka_${SCALA_VERSION}-${VERSION}"
TARBALL="core/build/distributions/kafka_${SCALA_VERSION}-${VERSION}.tgz"

LIBS_DIR="$RELEASE_DIR/libs"
LICENSE_FILE="$RELEASE_DIR/LICENSE"
LICENSES_DIR="$RELEASE_DIR/licenses"

exit_with_error() {
  echo "[Failed] $1"
  exit 1
}

renew_existing_release_dir() {
  local message="Renew existing $RELEASE_DIR release dir"
  echo "[Started] $message"
  rm -rf "$RELEASE_DIR"
  mkdir -p "$RELEASE_DIR"
  echo "[Finished] $message"
}

generate_tarball() {
  local message="Generate tarball"
  echo "[Started] $message"
  ./gradlew releaseTarGz --console=plain > /dev/null 2>&1 || exit_with_error "Error: run Gradle task"
  [[ -f "$TARBALL" ]] || exit_with_error "Tarball not generated: $TARBALL"
  echo "[Finished] $message"
}

extract_tarball() {
  local message="Extract tarball"
  echo "[Started] $message"
  tar -xzf "$TARBALL" -C . || exit_with_error "Error: extract tarball"
  [[ -d "$RELEASE_DIR" ]] || exit_with_error "Extracted directory not found: $RELEASE_DIR"
  echo "[Finished] $message"
}

list_jars() {
  local message="List JAR files"
  echo "[Started] $message"
  JARS=()
  for jar in "$LIBS_DIR"/*.jar; do
    if [[ ! $(basename "$jar") =~ ^(kafka|connect|trogdor) ]]; then
      JARS+=("$jar")
    fi
  done
  echo "[Finished] $message"
}

validate_if_there_are_missing_license_entries() {
  local message="Validate if there are missing entries in $LICENSE_FILE file"
  echo "[Started] $message"
  for jar in "${JARS[@]}"; do
    local jar_name="${jar##*/}"
    local jar_base="${jar_name%.jar}"
    grep -q "$jar_base" "$LICENSE_FILE" || exit_with_error "$jar_base is missing in LICENSE file"
  done
  echo "[Finished] $message"
}

validate_if_there_are_missing_license_files() {
  local message="Validate if there are missing license files in $LICENSES_DIR directory"
  echo "[Started] $message"
  for jar in "${JARS[@]}"; do
    local jar_name="${jar##*/}"
    local jar_base="${jar_name%.jar}"
    license_file="$LICENSES_DIR/$jar_base"
    [[ -f "$license_file" ]] || echo "[Warning] License file missing: $jar_base"
  done
  echo "[Finished] $message"
}

echo "[Started] Validate licenses"

cd ..

renew_existing_release_dir && echo "[Completed] Step 0/5"
generate_tarball && echo "[Completed] Step 1/5"
extract_tarball && echo "[Completed] Step 2/5"
list_jars && echo "[Completed] Step 3/5"
validate_if_there_are_missing_license_entries && echo "[Completed] Step 4/5"
validate_if_there_are_missing_license_files && echo "[Completed] Step 5/5"

echo "[Finished] Validate licenses"
