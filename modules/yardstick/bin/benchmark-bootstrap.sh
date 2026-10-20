#!/bin/bash

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

#
# Script that starts BenchmarkServer or BenchmarkDriver.
#

SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

if [ "${CUR_DIR}" != "" ]; then
    cd ${CUR_DIR}
fi

if [ "${MAIN_CLASS}" == "" ]; then
    echo "ERROR: Java class is not defined."
    echo "Type \"--help\" for usage."
    exit 1
fi

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    if [ "$JAVA_HOME" = "" ]; then
        JAVA=`which java`
        RETCODE=$?

        if [ $RETCODE -ne 0 ]; then
            echo "ERROR: JAVA_HOME environment variable is not found."
            echo "Please point JAVA_HOME variable to location of JDK 11 or later."
            echo "You can also download latest JDK at https://jdk.java.net"

            exit 1
        fi

        JAVA_HOME=
    else
        JAVA=${JAVA_HOME}/bin/java
    fi

    if [ ! -e "$JAVA" ]; then
        echo "ERROR: JAVA is not found in JAVA_HOME=$JAVA_HOME."
        echo "Please point JAVA_HOME variable to installation of JDK 11 or later."
        echo "You can also download latest JDK at https://jdk.java.net"

        exit 1
    fi

    JAVA_VER=`"$JAVA" -version 2>&1 | grep -i "version" | head -1 | sed -E 's/.*version "([^"]*)".*/\1/'`
    JAVA_MAJOR_VER=`echo "$JAVA_VER" | awk -F '[.+_-]' '{ if ($1 == "1") print $2; else print $1 }'`

    if [ -z "$JAVA_MAJOR_VER" ] || [ "$JAVA_MAJOR_VER" -lt 11 ] 2>/dev/null; then
        echo "ERROR: The version of JAVA installed in JAVA_HOME=$JAVA_HOME is incorrect."
        echo "Please point JAVA_HOME variable to installation of JDK 11 or later."
        echo "You can also download latest JDK at https://jdk.java.net"

        exit 1
    fi
}

#
# Discover path to Java executable and check it's version.
#
checkJava

ARGS=$*

CP=${CP}":${SCRIPT_DIR}/../libs/*"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms2g -Xmx2g -server -Djava.net.preferIPv4Stack=true "${JVM_OPTS}

#
# JDK specific options, required by Ignite for JDK 11 and later (see Ignite bin/include/jvmdefaults.sh).
#
if [ "${JAVA_MAJOR_VER}" -ge 11 ] && [ "${JAVA_MAJOR_VER}" -lt 15 ]; then
    JVM_OPTS="\
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
        --illegal-access=permit \
        ${JVM_OPTS}"
elif [ "${JAVA_MAJOR_VER}" -ge 15 ]; then
    JVM_OPTS="\
        --add-opens=java.base/jdk.internal.access=ALL-UNNAMED \
        --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
        --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
        --add-opens=java.base/java.io=ALL-UNNAMED \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        --add-opens=java.base/java.net=ALL-UNNAMED \
        --add-opens=java.base/java.util=ALL-UNNAMED \
        --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
        --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
        --add-opens=java.base/java.math=ALL-UNNAMED \
        --add-opens=java.sql/java.sql=ALL-UNNAMED \
        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
        --add-opens=java.base/java.time=ALL-UNNAMED \
        --add-opens=java.base/java.text=ALL-UNNAMED \
        --add-opens=java.management/sun.management=ALL-UNNAMED \
        --add-opens=java.desktop/java.awt.font=ALL-UNNAMED \
        ${JVM_OPTS}"
fi

#
# Assertions are disabled by default.
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="0"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS}" = "1" ]; then
    JVM_OPTS="${JVM_OPTS} -ea"
fi

if [ -z "$PROPS_ENV" ]; then
    if [ "$PROPS_ENV0" != "" ]; then
        export PROPS_ENV=$PROPS_ENV0
    fi
fi

ARGS=${ARGS}" --currentFolder ${CUR_DIR} --scriptsFolder ${SCRIPT_DIR}"

export JAVA

"$JAVA" ${JVM_OPTS} -cp ${CP} ${MAIN_CLASS} ${ARGS}
