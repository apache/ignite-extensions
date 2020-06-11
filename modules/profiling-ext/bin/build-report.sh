#!/usr/bin/env bash

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
# The script is used to create a performance report from profiling files collected from the cluster.
#
# Usage: build-report.sh path_to_profiling_files
#
# The path should contain only profiling files collected from the cluster.
# Profiling file name mask: node-${sys:nodeId}.prf
#
# For example:
# path_to_profiling_files
#   ├── node-162c7147-fef8-4ea2-bd25-8653c41fc7fa.prf
#   ├── node-7b8a7c5c-f3b7-46c3-90da-e66103c00001.prf
#   └── node-faedc6c9-3542-4610-ae10-4ff7e0600000.prf
#
# The report will be created in the new directory under the files path:
# path_to_profiling_files/report_yyyy-MM-dd_HH-mm-ss/
#

#
# Import common functions.
#
if [ "${IGNITE_HOME:-}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh
CP="${IGNITE_LIBS}:${IGNITE_HOME}/profiling/*:${IGNITE_HOME}/profiling/libs/*"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "${JVM_OPTS:-}" ] ; then
      JVM_OPTS="-Xms256m -Xmx8g"
fi

#
# Set main class to run script.
#
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.internal.profiling.ProfilingFilesParser
fi

#
# Garbage Collection options.
#
JVM_OPTS="\
    -XX:+UseG1GC \
     ${JVM_OPTS}"

#
# Run tool.
#
"$JAVA" ${JVM_OPTS} -cp "${CP}" ${MAIN_CLASS} $@
