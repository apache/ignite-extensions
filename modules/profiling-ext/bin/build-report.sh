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

CP="./libs/*"

JVM_OPTS="-Xms256m -Xmx32g"

MAIN_CLASS=org.apache.ignite.internal.perfstat.PerformanceReportBuilder

#
# Garbage Collection options.
#
JVM_OPTS="\
    -XX:+UseG1GC \
     ${JVM_OPTS}"

#
# Run tool.
#
Java ${JVM_OPTS} -cp "${CP}" ${MAIN_CLASS} $@
