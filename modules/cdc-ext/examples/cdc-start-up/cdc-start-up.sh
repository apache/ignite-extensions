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
# All-in-one CDC start-up manager. Use this script to run examples for CDC with Apache Ignite.
#

set -Eeuo pipefail
trap 'cleanup $LINENO' SIGINT SIGTERM ERR EXIT

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

IGNITE_BIN_DIR="${SCRIPT_DIR}/../../bin"
IGNITE_HOME="${SCRIPT_DIR}/../../"
IGNITE_LIBS="${SCRIPT_DIR}/../../libs"
IGNITE_CDC_EXAMPLES_DIR="${SCRIPT_DIR}/../config/cdc-start-up"

CURRENT_PID=$$

#
# Help message
#
usage() {
	cat <<EOF
This is a simple start-up script designed to ease the user experience with starting CDC for ignite clusters.

Available options:

-h, --help					Prints help summary
-i, --ignite igniteProperties			Starts a single node with provided properties. `
                                      `An ignite instance will be started with basic CDC configuration `
                                      `\$IGNITE_HOME/examples/config/cdc-start-up/cdc-base-configuration.xml

	Available options for igniteProperties include:
		* cluster-1
		* cluster-2

  Properties files are preconfigured for data replication between cluster-1 and cluster-2.

-c, --ignite-cdc consumerMode igniteProperties `
                                      `Starts CDC consumer with specified transfer mode to parse WAL archives `
                                      `from source cluster. igniteProperties is used to determine the source cluster.

	Available options for --ignite-cdc include:
		* ignite-to-ignite-thick	Creates a single thick client, `
		                          `used to transfer data from source-cluster to destination-cluster.
		* ignite-to-ignite-thin		Creates a single thin client, `
		                            `used to transfer data from source-cluster to destination-cluster.
		* ignite-to-kafka		Creates a cdc consumer, used to transfer data from source-cluster to specified Kafka topic.

-k, --kafka-to-ignite clientMode igniteProperties `
                                      `Starts Kafka topic consumer for data replication to destination cluster.

	Available options for --kafka-to-ignite include:
		* thick				Creates a single thick client, used to transfer data from Kafka to destination-cluster.
		* thin				Creates a single thin client, used to transfer data from Kafka to destination-cluster.

--check-cdc --key keyVal --value jsonVal [--cluster clusterNum] `
                                            `Starts CDC check with proposed (key, value) entry. `
                                            `The command puts the entry in the chosen cluster, and shows the comparison `
                                            `of caches between clusters as the entry reaches the other cluster.

	Options:
		* --key keyVal			Specifies key of the entry.
		* --value jsonVal		Specifies value of the entry as JSON. Example: '{"val": "val", "ver": "XXX"}'
		* --cluster clusterNum		Optional parameter for the cluster number (1 or 2) that initially stores the entry. `
		                                        `The default value is 1.
EOF
	exit
}

#
# General message output function
# Arguments:
#   1 - message to print
#
msg() {
	echo >&2 -e "${ORANGE}[PID=${CURRENT_PID-}]:${NOFORMAT} ${1-}"
}

#
# General message output function
# Arguments:
#   1 - entity for column 1
#   1 - entity for column 2
#
msgPrintf() {
  local val1="$1"
  local val2="$2"

  printf >&2 "${ORANGE}[PID=%s]:${NOFORMAT} | %-16s | %-16s |\n" "${CURRENT_PID-$$}" "${val1}" "${val2}"
}

#
# Exits with error
#
die() {
	local msg=$1
	local code=${2-1}
	msg "$msg"
	exit "$code"
}

#
# Colors setup function
#
setupColors() {
	if [[ -t 2 ]] && [[ -z "${NO_COLOR-}" ]] && [[ "${TERM-}" != "dumb" ]]; then
	  NOFORMAT='\033[0m' RED='\033[0;31m' GREEN='\033[0;32m' ORANGE='\033[0;33m'
	  BLUE='\033[0;34m' PURPLE='\033[0;35m' CYAN='\033[0;36m' YELLOW='\033[1;33m'
  else
    NOFORMAT='' RED='' GREEN='' ORANGE='' BLUE='' PURPLE='' CYAN='' YELLOW=''
	fi
}

#
# Script setup. Sets colors for messaging.
#
setup() {
	setupColors

	msg "${PURPLE}CDC start-up manager [PID=${CURRENT_PID-}]${NOFORMAT}"
}

#
# Clean-up function for exit and interruption
#
cleanup() {
	trap - SIGINT SIGTERM ERR EXIT

	msg "${PURPLE}CDC start-up manager [PID=${CURRENT_PID-}] is closed ${NOFORMAT}"
}

#
# General information message output function
# Arguments:
#   1 - message to print
#
infoMsg() {
	msg "${GREEN}${1-}${NOFORMAT}"
}

#
# Simple util function to check argument presence for specified parent argument
# Arguments:
#   1 - parent command argument
#   2 - argument name to check
#   3 - argument to check
#
checkMissing() {
	local parent_arg_name=$1
	local arg_name=$2

	local arg_to_check=$3

	[[ -z $arg_to_check ]] && die "Missing script argument [""${arg_name-}""] for ""${parent_arg_name-}""!"

	return 0
}

#
# Checks --ignite arguments
# Globals:
#   ignite_properties_path - '.properties' holder path. The file is used to configure server node
# Arguments:
#   "$@" - script command arguments
#
checkServerParams() {
  checkMissing "${script_param-}" "igniteProperties" "${2-}"

  ignite_properties_path="${IGNITE_CDC_EXAMPLES_DIR}"/${2-}
}

#
# Checks --ignite-cdc arguments
# Globals:
#   consumer_mode - Transfer type for CDC
#   cdc_streamer_xml_file_name - '.xml' filename of the specified transfer type
#   ignite_properties_path - '.properties' holder path. The file is used to configure CDC consumer
# Arguments:
#   "$@" - script command arguments
#
checkConsumerParams() {
	consumer_mode=${2-}

	checkMissing "${script_param-}" "consumerMode" "${consumer_mode-}"

	case $consumer_mode in
		ignite-to-ignite-thick) export cdc_streamer_xml_file_name="cdc-streamer-I2I.xml" ;;
		ignite-to-ignite-thin) export cdc_streamer_xml_file_name="cdc-streamer-I2I-thin.xml" ;;
		ignite-to-kafka) export cdc_streamer_xml_file_name="cdc-streamer-I2K.xml" ;;
		*) die "Unknown consumer mode for CDC: ${consumer_mode-}" ;;
	esac

	checkMissing "${consumer_mode-}" "igniteProperties" "${3-}"

	ignite_properties_path="${IGNITE_CDC_EXAMPLES_DIR}"/${3-}

	return 0
}

#
# Checks --kafka-to-ignite arguments
# Globals:
#   client_mode - Transfer type for CDC
#   cdc_streamer_xml_file_name - '.xml' filename of the specified transfer type
#   ignite_properties_path - '.properties' holder path. The file is used to configure CDC client
# Arguments:
#   "$@" - script command arguments
#
checkKafkaConsumerParams() {
	client_mode=${2-}

	checkMissing "${script_param-}" "clientMode" "${client_mode-}"

	case $client_mode in
		thick) ;;
		thin) ;;
		*) die "Unknown client mode for CDC: ${client_mode-}" ;;
	esac

	checkMissing "${client_mode-}" "igniteProperties" "${3-}"

	ignite_properties_path="${IGNITE_CDC_EXAMPLES_DIR}"/${3-}

	return 0
}

#
# Checks --check-cdc arguments
# Globals:
#   key - Entity key
#   value - Entity value in JSON
#   cluster - Cluster to put entity in
# Arguments:
#   "$@" - script command arguments
#
checkEntriesParams() {
	cluster=1
	local jsonValue=""

	while :; do
		case "${2-}" in
			--key) key="${3-}"; shift ;;
			--value) jsonValue="${3-}"; shift ;;
			--cluster) cluster="${3-}"; shift ;;
			-?*) die "Unknown option: ${script_param-}" ;;
			*) break ;;
		esac
		shift
	done

	checkMissing "${script_param-}" "key" "${key-}"
	checkMissing "${script_param-}" "value" "${jsonValue-}"

	value=$(echo $jsonValue | awk -F'"val": ' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')
	version=$(echo $jsonValue | awk -F'"ver": ' '{ print $2 }' | awk -F'}' '{ print $1 }')

	checkMissing "${jsonValue}" "val field" "${value-}"

	return 0
}

#
# Checks if all required optional libraries enabled for CDC check
#
checkLibraries() {
  local lib1="ignite-rest-http";
  local lib2="ignite-json";

  if [ ! -d "$IGNITE_LIBS/$lib1" ] && [ ! -d "$IGNITE_LIBS/$lib2" ]; then
    die "${RED}Failure! Check that $lib1 and $lib2 optional libraries are enabled.";
  elif [ ! -d "$IGNITE_LIBS/$lib1" ]; then
    die "${RED}Failure! Check that $lib1 optional library is enabled.";
  elif [ ! -d "$IGNITE_LIBS/$lib2" ]; then
    die "${RED}Failure! Check that $lib2 optional library is enabled.";
  fi
}

#
# Starts single Ignite instance
# cdc-base-configuration needs dummy streamer to start, which is why cdc_streamer_xml_file_name is exported
#
startIgnite() {
	infoMsg "Starting Ignite for ${ignite_properties_path-}"

	export cdc_streamer_xml_file_name="cdc-streamer-I2I.xml"
	export ignite_properties_path

	"${IGNITE_BIN_DIR}"/ignite.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-base-configuration.xml
}

#
# Starts single CDC consumer instance
#
startCdcConsumer() {
	infoMsg "Starting CDC consumer for ${ignite_properties_path-} with ${consumer_mode-}"

	export ignite_properties_path
	export IGNITE_HOME

	source "${IGNITE_BIN_DIR}"/ignite-cdc.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-base-configuration.xml ;
}

#
# Starts single Kafka consumer instance
#
startKafkaConsumer() {
	infoMsg "Starting Kafka topic consumer for ${ignite_properties_path-} with ${client_mode-}"

	export ignite_properties_path
	export IGNITE_HOME

	case $client_mode in
		thick) source "${IGNITE_BIN_DIR}"/kafka-to-ignite.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-streamer-K2I.xml ;;
		thin) source "${IGNITE_BIN_DIR}"/kafka-to-ignite.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-streamer-K2I-thin.xml ;;
		*) die "Unknown client mode for CDC: ${client_mode-}" ;;
	esac
}

#
# Prints delimiter
#
printLine() {
	msg "+------------------+------------------+"
}

#
# Prints 'cluster' line
#
printClusterMsg() {
	msgPrintf "cluster-1" "cluster-2"
}

#
# Fetches values from clusters
# Globals:
#   value1 - Entry value from cluster 1
#   version1 - Entry version from cluster 1
#   value2 - Entry value from cluster 2
#   version2 - Entry version from cluster 2
#
updateValues() {
	local json1=$(curl -s 'http://localhost:8080/ignite?cmd=get&key='$key'&cacheName=terminator&keyType=String&valueType=IgniteBiTuple' -X GET -H 'Content-Type: application/x-www-form-urlencoded')
	local json2=$(curl -s 'http://localhost:8081/ignite?cmd=get&key='$key'&cacheName=terminator&keyType=String&valueType=IgniteBiTuple' -X GET -H 'Content-Type: application/x-www-form-urlencoded')

  value1=$(echo $json1 | awk -F'val1":' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')
  version1=$(echo $json1 | awk -F'val2":' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')

  value2=$(echo $json2 | awk -F'val1":' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')
  version2=$(echo $json2 | awk -F'val2":' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')

	if [[ -z $value1 || $value1 == "" ]]; then
	  value1="-"; version1="-";
	fi

	if [[ -z $value2 || $value2 == "" ]]; then
	  value2="-"; version2="-";
	fi
}

#
# Prints the values from clusters for specified key
#
printValuesPair() {
	updateValues

	msgPrintf "val: ${value1-}" "val: ${value2-}"
	msgPrintf "ver: ${version1-}" "ver: ${version2-}"
}

#
# Use a function to construct the data payload based on whether version is set
#
constructPayload() {
  local value=$1
  local version=$2
  local payload="%7B%22val1%22%3A$value" # always include val1

  if [[ -n "$version" ]]; then  # Check if version is not empty
    payload="${payload}%2C%22val2%22%3A$version"  # Add val2 if version is set
  fi

  payload="${payload}%7D" # close the json
  echo "$payload"
}

#
# Determine the correct port based on the cluster value
#
getPort() {
  if [[ "$cluster" == "1" ]]; then
    echo "8080"
  else
    echo "8081"
  fi
}

#
# Puts the entry in the specified cluster
#
pushEntry() {
	msg "Pushing entry..."

	local result

	# Main logic
	payload=$(constructPayload "$value" "$version")
	port=$(getPort)

	url="http://localhost:${port}/ignite?cmd=put&key=${key}&val=$payload&cacheName=terminator&keyType=String&valueType=IgniteBiTuple"

	result=$(curl -s "$url" -X POST -H 'Content-Type: application/x-www-form-urlencoded')

	local successStatus=$(echo $result | awk -F'successStatus":' '{ print $2 }' | awk -F',' '{ print $1 }' | awk -F'}' '{print $1}')
	local errorMsg=$(echo $result | awk -F'"error":"' '{gsub(/\\n.*/,"",$2); gsub(/.*reason=/,"",$2); print $2}')

	if ((successStatus > 0)); then
	  msg ""; die "${RED}${errorMsg}${NOFORMAT}";
	fi

	msg "Success"
	msg ""
}

#
# Prints the values from clusters for specified key and increments counter for CDC check
#
iterateValuesCheck() {
  printValuesPair

  printLine

  current_time_s=$(date +%s)
}

#
# Trails clusters entries for specified key
#
printValuesUntilSuccess() {
	local start_time_s=$(date +%s)
	declare current_time_s=$(date +%s)

	while iterateValuesCheck; ([[ $value1 != $value2 || $version1 != $version2 ]]) && ((current_time_s - start_time_s <= 60)); do
		sleep 1
	done

	if ((current_time_s - start_time_s > 60)); then
	  msg ""; die "${RED}Replication timed out! Check CDC cycle${NOFORMAT}";
	fi

	msg ""
	msg "Success"
}

#
# General function to check CDC entry transport
#
performCheck() {
	infoMsg "CDC check started"
	msg ""

	printLine
	printClusterMsg
	printLine
	printValuesPair
	printLine
	msg ""

	pushEntry

	printLine
	printClusterMsg
	printLine
	printValuesUntilSuccess

	return 0
}

#
# Main function to parse commands arguments
#
parseParams() {
	script_param=${1-}

	[[ -z $script_param ]] && die "Missing script parameter! Use --help to see available options."

	case $script_param in
		-i | --ignite)
			checkServerParams "$@"
			checkLibraries
			startIgnite
			;;
		-c | --ignite-cdc)
			checkConsumerParams "$@"
			checkLibraries
			startCdcConsumer
			;;
		-k | --kafka-to-ignite)
			checkKafkaConsumerParams "$@"
			checkLibraries
			startKafkaConsumer
			;;
		--check-cdc)
			checkEntriesParams "$@"
			checkLibraries
			performCheck
			;;
		-h | --help) usage ;;
		-?*) die "Unknown option: ${script_param-}" ;;
		*) die "Unknown input: ${script_param-}" ;;
	esac

	return 0
}

#
# Script Main Body
#

setup

parseParams "$@"
