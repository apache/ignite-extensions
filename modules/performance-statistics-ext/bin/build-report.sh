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
# The script is used to create a performance report from performance statistics files collected from the cluster.
#

SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

source "${SCRIPT_DIR}"/include/jvmdefaults.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms256m -Xmx32g"

#
# Final JVM_OPTS for Java 9+ compatibility
#
JVM_OPTS=$(getJavaSpecificOpts $version "$JVM_OPTS")


#
# Define classpath
#
CP="${SCRIPT_DIR}/../libs/ignite-performance-statistics-ext/*"

#
# Set main class to run the tool.
#
MAIN_CLASS=org.apache.ignite.internal.performancestatistics.PerformanceStatisticsReportBuilder

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
