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

module_version="1.0.0"
dir_name="release-module-name" #enter module name to release.
module_name="ignite-${dir_name}"
dir="../modules/${dir_name}"

server_url="https://repository.apache.org/service/local/staging/deploy/maven2"
server_id="apache.releases.https"

echo "Uploading $module_name to staging"

now=$(date +'%H%M%S')

main_file=$(find $dir/target -name "${module_name}-${module_version}.jar")
pom=$(find $dir -name "pom-installed.xml")
javadoc=$(find $dir/target -name "${module_name}-${module_version}-javadoc.jar")
sources=$(find $dir/target -name "${module_name}-${module_version}-sources.jar")
tests=$(find $dir -name "${module_name}-${module_version}-tests.jar")

adds=""

echo "Uploading ${dir}."

if [[ $javadoc == *javadoc* ]]
then
	adds="${adds} -Djavadoc=${javadoc}"
fi

if [[ $sources == *sources* ]]
then
	adds="${adds} -Dsources=${sources}"
fi

if [[ $tests == *tests* ]]
then
	adds="${adds} -Dfiles=${tests} -Dtypes=jar -Dclassifiers=tests"
fi

if [[ ! -n $main_file && ! -n $features ]]
then
	main_file=$pom
	adds="-Dpackaging=pom"
fi

echo "Directory: $dir"
echo "File: $main_file"
echo "Adds: $adds"

mvn gpg:sign-and-deploy-file -Papache_staging -Dfile=$main_file -Durl=$server_url -DrepositoryId=$server_id -DretryFailedDeploymentCount=10 -DpomFile=$pom ${adds} --settings ./settings.xml

result="Uploaded"

while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *ERROR* ]]
    then
        result="Uploading failed. Please check log file: ${logname}."
    fi
done < ./$logname

echo $result

echo " "
echo "======================================================"
echo "Maven staging should be created"
echo "Please check results at"
echo "https://repository.apache.org/#stagingRepositories"
echo "Don't forget to close staging with proper comment"
