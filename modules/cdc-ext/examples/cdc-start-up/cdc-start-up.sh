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

-h, --help						Prints help summary
-i, --ignite igniteProperties			  Starts a single node with provided properties. `
                                      `An ignite instance will be started with basic persistence configuration with CDC.

    * igniteProperties  ignite properties folder under \$IGNITE_HOME/examples/config/cdc-start-up

-c, --cdc-client clientMode ignitePropertiesPath `
                                      `Starts CDC client with specified transfer mode.

	Available options for --cdc-client include:
		* --ignite-to-ignite			Creates a single server client (Thick client), `
		                          `used to transfer data from source-cluster to destination-cluster.
		* --ignite-to-ignite-thin		Creates a single thin client, `
		                            `used to transfer data from source-cluster to destination-cluster.
		* --ignite-to-kafka			Creates a cdc client, used to transfer data from source-cluster to specified Kafka topics.
		* --kafka-to-ignite			Creates a single server client (Thick client), `
		                        `used to transfer data from Kafka to destination-cluster.
		* --kafka-to-ignite-thin		Creates a single thin client, used to transfer data from Kafka to destination-cluster.

--check-cdc --key intNum1 --value intNum2 --version intNum3 [--cluster clusterNum] `
                                            `Starts CDC check with proposed (key, value) entry. `
                                            `The command puts the entry in the chosen cluster, and shows the comparison `
                                            `of caches between clusters as the entry reaches the other cluster.
		Options:
			* --key intNum1		Specifies key of the entry.
			* --value intNum2		Specifies value of the entry.
			* --version intNum3		Specifies version of the entry. The value is used to resolve conflicted entries.
			* --cluster clusterNum		Optional parameter for the cluster number (1 or 2) that initially stores the entry.
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
  checkMissing "${script_param-}" "ignitePropertiesPath" "${2-}"

  ignite_properties_path="${IGNITE_CDC_EXAMPLES_DIR}"/${2-}
}

#
# Checks --cdc-client arguments
# Globals:
#   client_mode - Transfer type for CDC
#   cdc_streamer_xml_file_name - '.xml' filename of the specified transfer type
#   ignite_properties_path - '.properties' holder path. The file is used to configure CDC client
# Arguments:
#   "$@" - script command arguments
#
checkClientParams() {
	client_mode=${2-}

	checkMissing "${script_param-}" "clientMode" "${client_mode-}"

	case $client_mode in
		--ignite-to-ignite) export cdc_streamer_xml_file_name="cdc-streamer-I2I.xml" ;;
		--ignite-to-ignite-thin) export cdc_streamer_xml_file_name="cdc-streamer-I2I-thin.xml" ;;
		--ignite-to-kafka) export cdc_streamer_xml_file_name="cdc-streamer-I2K.xml" ;;
		--kafka-to-ignite) ;;
		--kafka-to-ignite-thin) ;;
		*) die "Unknown client mode for CDC: ${client_mode-}" ;;
	esac

	checkMissing "${client_mode-}" "ignitePropertiesPath" "${3-}"

	ignite_properties_path="${IGNITE_CDC_EXAMPLES_DIR}"/${3-}

	return 0
}

#
# Checks --check-cdc arguments
# Globals:
#   key - Entity key
#   value - Entity value
#   cluster - Cluster to put entity in
#   version - Entity version. Used to resolve conflict entries
# Arguments:
#   "$@" - script command arguments
#
checkEntriesParams() {
	cluster=1

	while :; do
		case "${2-}" in
			--key) key="${3-}"; shift ;;
			--value) value="${3-}"; shift ;;
			--cluster) cluster="${3-}"; shift ;;
			--version) version="${3-}"; shift ;;
			-?*) die "Unknown option: ${script_param-}" ;;
			*) break ;;
		esac
		shift
	done

	checkMissing "${script_param-}" "key" "${key-}"
	checkMissing "${script_param-}" "value" "${value-}"
	checkMissing "${script_param-}" "version" "${version-}"

	return 0
}

#
# Checks if library is enabled
# Arguments:
#   1 - library name
#
checkLibrary() {
	local lib_to_check=${1-}

	if [ ! -d "$IGNITE_LIBS/$lib_to_check" ]; then
	  die "${RED}Failure! Check that $lib_to_check optional library is enabled. Restart clusters if necessary";
	fi
}

#
# Checks if all required optional libraries enabled for CDC check
#
checkLibraries() {
	checkLibrary "ignite-rest-http"
	checkLibrary "ignite-json"
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
# Starts single CDC client instance
#
startCDCClient() {
	infoMsg "Starting CDC client for ${ignite_properties_path-} with ${client_mode-}"

	export ignite_properties_path
	export IGNITE_HOME

	case $client_mode in
		--kafka-to-ignite) source "${IGNITE_BIN_DIR}"/kafka-to-ignite.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-streamer-K2I.xml ;;
		--kafka-to-ignite-thin) source "${IGNITE_BIN_DIR}"/kafka-to-ignite.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-streamer-K2I-thin.xml ;;
		*) source "${IGNITE_BIN_DIR}"/ignite-cdc.sh -v "${IGNITE_CDC_EXAMPLES_DIR}"/cdc-base-configuration.xml ;;
	esac
}

#
# Prints delimiter
#
printLine() {
	msg "+--------------------------+"
}

#
# Prints 'cluster' line
#
printClusterMsg() {
	msg "\tcluster1\tcluster2"
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
	local json1=$(curl -s 'http://localhost:8080/ignite?cmd=get&key='$key'&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X GET -H 'Content-Type: application/x-www-form-urlencoded')
	local json2=$(curl -s 'http://localhost:8081/ignite?cmd=get&key='$key'&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X GET -H 'Content-Type: application/x-www-form-urlencoded')

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

	msg "|\tval: ${value1-}\t|\tval: ${value2-}\t|"
	msg "|\tver: ${version1-}\t|\tver: ${version2-}\t|"
}

#
# Puts the entry in the specified cluster
#
pushEntry() {
	msg "Pushing entry..."

	local result

	if [[ "$cluster" == "1" ]]; then
	  result=$(curl -s 'http://localhost:8080/ignite?cmd=put&key='$key'&val=%7B%22val1%22%3A'$value'%2C%22val2%22%3A'$version'%7D&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X POST -H 'Content-Type: application/x-www-form-urlencoded')
	else
	  result=$(curl -s 'http://localhost:8081/ignite?cmd=put&key='$key'&val=%7B%22val1%22%3A'$value'%2C%22val2%22%3A'$version'%7D&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X POST -H 'Content-Type: application/x-www-form-urlencoded')
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

  ((count++))
}

#
# Trails clusters entries for specified key
#
printValuesUntilSuccess() {
	declare -i count=0

	while iterateValuesCheck; ([[ $value1 != $value2 || $version1 != $version2 ]]) && ((count < 20)); do
		  true
	done

	if (( count >= 20 )); then
	  msg ""; die "${RED}Failure! Check CDC cycle${NOFORMAT}";
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

	printClusterMsg
	printLine
	printValuesPair
	printLine
	msg ""

	pushEntry

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
			startIgnite
			;;
		-c | --cdc-client)
			checkClientParams "$@"
			startCDCClient
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
