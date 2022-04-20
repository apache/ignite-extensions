#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Run from the Apache Ignite Extensions root directory.
# Usage: ./scripts/extension-deploy.sh modules/zookeeper-ip-finder-ext/
#

if [ $# -eq 0 ]
  then
    echo "Ignite Extension directory is not specified."
    exit 1
fi

GIT_HOME="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
SCRIPTS_HOME="${GIT_HOME}/scripts/"

# Import patch functions.
. ${SCRIPTS_HOME}/git-patch-functions.sh

#
now=$(date +'%H%M%S')
server_id="apache.releases.https"
dir=$1
module_name="ignite-$(sed 's/\/$//' <<< $1 |  cut -d '/' -f2)"
log=$(pwd)"/log_${module_name}_${now}.log"

echo "Extension Module Name:    ${module_name}" | tee ${log}

cd ${dir}

ext_ver=$(mvn help:evaluate -D expression=project.version -q -DforceStdout)
ignite_ver=$(mvn help:evaluate -D expression=ignite.version -q -DforceStdout)

echo "Extension Version:        ${ext_ver}" | tee -a ${log}
echo "Extension Ignite Version: ${ignite_ver}" | tee -a ${log}

# Get the RC tag associated with the last commit in the current branch.
rc_tag=$(git describe --tags --exact-match --abbrev=0)

if [[ rc_tag =~ "${module_name}-${ext_ver}-rc"* ]]; then
  echo "ERROR: The RC tag must have the following format: ignite-zookeeper-if-finder-ext-1.0.0-rc1"
  echo "ERROR: Given tag: ${rc_tag}"

  exit 1;
fi

echo "Extension RC tag:         ${rc_tag}" | tee -a ${log}
echo "Start Maven Build ..." } | tee -a ${log}

requireCleanWorkTree ${GIT_HOME}

# Build the Extension
mvn clean install -DskipTests -Pextension-release | tee -a ${log}

while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *ERROR* ]]; then
        result="ERROR: building. Please check log file: ${log}."

        exit 1;
    fi
done < ./${log}

cd target



#echo "RC ${ignite_version}${rc_name}"
# Uncomment subsequent line in case you want to remove incorrectly prepared RC
#svn rm -m "Removing redundant Release" https://dist.apache.org/repos/dist/dev/ignite/$ignite_version$rc_name || true
#svn import svn/vote https://dist.apache.org/repos/dist/dev/ignite/$ignite_version$rc_name -m "New RC ${ignite_version}${rc_name}: Binaries"

#
# Output result and notes
#
echo
echo "============================================================================="
echo "Artifacts should be moved to RC repository"
echo "Please check results at:"
echo " * binaries: https://dist.apache.org/repos/dist/dev/ignite/ignite-extensions/${ignite_version}${rc_name}"