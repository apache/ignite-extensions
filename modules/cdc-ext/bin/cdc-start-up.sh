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
trap cleanup SIGINT SIGTERM ERR EXIT

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
IGNITE_LOG_DIR="${SCRIPT_DIR}/../work/log"
IGNITE_CDC_EXAMPLE_DIR="${SCRIPT_DIR}/../examples/config/cdc-start-up"

CURRENT_PID=$$

#
# Help message
#
usage() {
	cat <<EOF
This is a simple start-up script designed to ease the user experience with starting CDC for ignite clusters.

Available options:

-h, --help						Prints help summary
-i, --ignite ignitePropertiesPath			Starts a single node with provided properties. `
                                      `An ignite instance will be started with basic persistence configuration with CDC.
-c, --cdc-client clientMode [--activate-cluster] ignitePropertiesPath `
                                      `Starts CDC client with specified transfer mode. `
                                      `Use --activate-cluster to activate both clusters: source and destination.

	Available options for cdc client mode include:
		* --ignite-to-ignite			Creates a single server client (Thick client), `
		                          `used to transfer data from source-cluster to destination-cluster.
		* --ignite-to-ignite-thin		Creates a single thin client, `
		                            `used to transfer data from source-cluster to destination-cluster.
		* --ignite-to-kafka			Creates a cdc client, used to transfer data from source-cluster to specified Kafka topics.
		* --kafka-to-ignite			Creates a single server client (Thick client), `
		                        `used to transfer data from Kafka to destination-cluster.
		* --kafka-to-ignite-thin		Creates a single thin client, used to transfer data from Kafka to destination-cluster.

--active-passive [--with-kafka] [--thin] ignitePropertiesPath1 ignitePropertiesPath2
		Starts clusters with an active-passive replication strategy. The default data transfer strategy is 'ignite-to-ignite'.
			* --with-kafka		    Used for replication with Kafka.
			* --thin		    Indicates thin clients usage for CDC process.
			* ignitePropertiesPath1, ignitePropertiesPath1  Paths to configuration properties files. `
			                                                `Example: ../examples/config/cdc-start-up/cluster-1

--active-active [--with-kafka] [--thin] ignitePropertiesPath1 ignitePropertiesPath2
		Starts clusters with an active-active replication strategy. The default data transfer strategy is 'ignite-to-ignite'.
			* --with-kafka		    Used for replication with Kafka.
      * --thin		    Indicates thin clients usage for CDC process.
      * ignitePropertiesPath1, ignitePropertiesPath1  Paths to configuration properties files. `
                                                      `Example: ../examples/config/cdc-start-up/cluster-1

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
# Script setup. Declares PIDs array for subprocesses, and sets colors for messaging.
# Globals:
#   PIDs - holds IDs of all subprocesses
#
setup() {
	declare -a PIDs

	setupColors

	msg "${PURPLE}CDC start-up manager [PID=${CURRENT_PID-}]${NOFORMAT}"
}

#
# Clean-up function for exit and interruption
#
cleanup() {
	trap - SIGINT SIGTERM ERR EXIT

	for i in ${PIDs[@]}; do
		kill -TERM $i
		unset 'PIDs[@]'
	done

	wait

	msg "${PURPLE}CDC start-up manager [PID=${CURRENT_PID-}] is closed ${NOFORMAT}"
}

#
# Adds subprocess ID to PIDs
#
addProcess() {
	PIDs+=($!)
}

#
# Start-up success message
#
getProcessInfo() {
	infoMsg "CDC script [PID=${CURRENT_PID-}] for ${script_param-} has started."
	infoMsg "\nSubtasks:"

	for i in ${PIDs[@]}; do
	  ps -p $i
	done
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

	[[ -z $arg_to_check ]] && die "Missing script argument ["${arg_name-}"] for "${parent_arg_name-}"!"

	return 0
}

#
# Checks --cdc-client arguments
# Globals:
#   client_mode - Transfer type for CDC
#   cdc_streamer_xml_file_name - '.xml' filename of the specified transfer type
#   with_activate_cluster - cluster activation flag. Specifies whether to activate clusters (source and destination)
#   user_ignite_properties_path - '.properties' holder path. The file is used to configure CDC client
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

	with_activate_cluster=false

	if [[ "$3" == "--activate-cluster" ]]; then
	  with_activate_cluster=true; shift;
	fi

	user_ignite_properties_path=${3-}

	checkMissing "${client_mode-}" "ignitePropertiesPath" "${user_ignite_properties_path-}"

	return 0
}

#
# Checks --active-passive or --active-active arguments
# Globals:
#   with_kafka - CDC transfer type flag. Indicates whether Kafka will be used for CDC
#   with_thin - CDC transfer type flag. Indicates whether thin clients will be used to connect to destination clusters
#   user_ignite_properties_path1 - '.properties' holder path. The file is used to configure CDC client for cluster 1
#   user_ignite_properties_path2 - '.properties' holder path. The file is used to configure CDC client for cluster 2
# Arguments:
#   "$@" - script command arguments
#
checkParams() {
	checkMissing "${script_param-}" "ignitePropertiesPath1" "${2-}"

	with_kafka=false
	with_thin=false

	if [[ "$2" == "--with-kafka" ]]; then
	  with_kafka=true; infoMsg "Transfer: through Kafka"; shift;
	fi

	checkMissing "${script_param-}" "ignitePropertiesPath1" "${2-}"

	if [[ "$2" == "--thin" ]]; then
	  with_thin=true; infoMsg "Thin clients are used"; shift;
	fi

	user_ignite_properties_path1=${2-}
	user_ignite_properties_path2=${3-}

	checkMissing "${script_param-}" "ignitePropertiesPath1" "${user_ignite_properties_path1-}"
	checkMissing "${script_param-}" "ignitePropertiesPath2" "${user_ignite_properties_path2-}"

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
# Waits for specific line to appear in '.log' file
# Arguments:
#   1 - '.log' file path
#   2 - line to look for in the '.log' file
#
waitForLine() {
	local log_file=${1-}
	local line=${2-}

	while [[ ! -f $log_file ]] || [[ ! -e $log_file ]] || [[ ! -s $log_file ]]; do
	  sleep 1s
	done

	until grep -q -s $line $log_file; do
	  sleep 1s
	done
}

#
# Starts single Ignite instance
#
startIgnite() {
	infoMsg "Starting Ignite for ${user_ignite_properties_path-}"

	export cdc_streamer_xml_file_name="cdc-streamer-I2I.xml"
	export ignite_properties_path="$SCRIPT_DIR/$user_ignite_properties_path"

	"${SCRIPT_DIR}"/ignite.sh "${IGNITE_CDC_EXAMPLE_DIR}"/cdc-base-configuration.xml
}

#
# Activates source and destination clusters for CDC client (--cdc-client)
# Sourced ignites properties from '.properties':
#   server_connector_port - port for control.sh connection to source cluster
#   destination_connector_port - port for control.sh connection to destination cluster
#
activateClusters() {
	infoMsg "Clusters will be activated"

	source "$ignite_properties_path/ignite-cdc.properties"

	infoMsg "Activating source cluster with localhost:${server_connector_port-} for ${client_mode} CDC client"
	"${SCRIPT_DIR}"/control.sh --set-state ACTIVE --host localhost:"${server_connector_port-}" --yes
	infoMsg "Activating destination cluster with localhost:${destination_connector_port-} for ${client_mode} CDC client"
	"${SCRIPT_DIR}"/control.sh --set-state ACTIVE --host localhost:"${destination_connector_port-}" --yes

	return 0
}

#
# Starts single CDC client instance
#
startCDCClient() {
	infoMsg "Starting CDC client for ${user_ignite_properties_path-} with ${client_mode-}"

	export ignite_properties_path="$SCRIPT_DIR/$user_ignite_properties_path"

	if [[ "$with_activate_cluster" == "true" ]]; then
	  activateClusters
	fi

	case $client_mode in
		--kafka-to-ignite) source "${SCRIPT_DIR}"/kafka-to-ignite.sh "${IGNITE_CDC_EXAMPLE_DIR}"/cdc-streamer-K2I.xml ;;
		--kafka-to-ignite-thin) source "${SCRIPT_DIR}"/kafka-to-ignite.sh "${IGNITE_CDC_EXAMPLE_DIR}"/cdc-streamer-K2I-thin.xml ;;
		*) source "${SCRIPT_DIR}"/ignite-cdc.sh "${IGNITE_CDC_EXAMPLE_DIR}"/cdc-base-configuration.xml ;;
	esac
}

#
# Starts source and destination ignite cluster nodes
#
startTwoIgnites() {
	local log_file1="$IGNITE_LOG_DIR/log_file1.log"
	local log_file2="$IGNITE_LOG_DIR/log_file2.log"

	"${SCRIPT_DIR}"/cdc-start-up.sh -i $user_ignite_properties_path1 > $log_file1 & addProcess

	waitForLine $log_file1 "Ignite node started OK"

	infoMsg "Ignite instance has successfully started [logFile=${log_file1}]"

	"${SCRIPT_DIR}"/cdc-start-up.sh -i $user_ignite_properties_path2 > $log_file2 & addProcess

	waitForLine $log_file2 "Ignite node started OK"

	infoMsg "Ignite instance has successfully started [logFile=${log_file2}]"

	return 0;
}

#
# Checks whether --activate-cluster argument is needed for ./cdc-start-up --cdc-client.
# Clusters are activated only for cluster 1 clients ignition.
# Globals:
#   activate_cmd - Either empty or --activate-cluster.
#                  Used to specify cluster activation during --active-passive or --active-active
# Arguments:
#   1 - Cluster number
#
checkClusterActivation() {
	local cluster_num=${1-}

	if [[ ! -z ${activate_cmd+x} ]]; then
	  unset activate_cmd
	fi

	case ${cluster_num-} in
		"1") activate_cmd="--activate-cluster" ;;
		*) ;;
	esac

	return 0;
}

#
# Starts Kafka source and destination CDC clients
# Arguments:
#   1 - '.properties' file path for source cluster
#   2 - '.properties' file path for destination cluster
#   3 - Source cluster number
#   4 - Destination cluster number
#
startKafkaClients() {
	local user_ignite_properties_path1=${1-}
	local user_ignite_properties_path2=${2-}

	local cluster_num_src=${3-}
	local cluster_num_dest=${4-}

	local cdc_client_source_log="$IGNITE_LOG_DIR/ignite-to-kafka-${cluster_num_src-}.log"
	local cdc_client_destination_log=""

	checkClusterActivation $cluster_num_src

	"${SCRIPT_DIR}"/cdc-start-up.sh -c --ignite-to-kafka ${activate_cmd+"$activate_cmd"} \
	                                              "${user_ignite_properties_path1-}" > $cdc_client_source_log & addProcess

	waitForLine $cdc_client_source_log "Ignite documentation:"

	infoMsg "CDC client instance has successfully started [clientMode=ignite-to-kafka, logFile=${cdc_client_source_log}]"

	if [[ "$with_thin" == "true" ]]; then
		cdc_client_destination_log="$IGNITE_LOG_DIR/kafka-to-ignite-thin-${cluster_num_dest-}.log"

		"${SCRIPT_DIR}"/cdc-start-up.sh -c --kafka-to-ignite-thin \
		                                      "${user_ignite_properties_path2-}" > $cdc_client_destination_log & addProcess
	else
		cdc_client_destination_log="$IGNITE_LOG_DIR/kafka-to-ignite-${cluster_num_dest-}.log"

		"${SCRIPT_DIR}"/cdc-start-up.sh -c --kafka-to-ignite \
		                                      "${user_ignite_properties_path2-}" > $cdc_client_destination_log & addProcess
	fi

	waitForLine $cdc_client_destination_log "Ignite documentation:"

	infoMsg "CDC client instance has successfully started [clientMode=kafka-to-ignite, withThin=${with_thin}, `
	                                                                              `logFile=${cdc_client_destination_log}]"

	return 0;
}

#
# Starts Ignite source and destination CDC clients
# Arguments:
#   1 - '.properties' file path for source cluster
#   2 - Source cluster number
#
startIgniteClients() {
	local cdc_client_log=""

	local user_ignite_properties_path=${1-}
	local cluster_num=${2-}

	checkClusterActivation $cluster_num

	if [[ "$with_thin" == "true" ]]; then
		cdc_client_log="$IGNITE_LOG_DIR/ignite-to-ignite-thin-${cluster_num-}.log"
		log_message="Ignite documentation:"

		"${SCRIPT_DIR}"/cdc-start-up.sh -c --ignite-to-ignite-thin ${activate_cmd+"$activate_cmd"} \
		                                                    "${user_ignite_properties_path-}" > $cdc_client_log & addProcess
	else
		cdc_client_log="$IGNITE_LOG_DIR/ignite-to-ignite-${cluster_num-}.log"
		log_message="Ignite node started OK"

		"${SCRIPT_DIR}"/cdc-start-up.sh -c --ignite-to-ignite ${activate_cmd+"$activate_cmd"} \
		                                                    "${user_ignite_properties_path-}" > $cdc_client_log & addProcess
	fi

	waitForLine $cdc_client_log $log_message

	infoMsg "CDC client instance has successfully started [clientMode=ignite-to-ignite, withThin=${with_thin}, `
  	                                                                              `logFile=${cdc_client_log}]"

	return 0;
}

#
# Starts Active-Passive CDC
#
startActivePassive() {
	infoMsg "Starting Active-Passive replication"

	mkdir -p "$IGNITE_LOG_DIR"

	local cluster_num_src="1"
	local cluster_num_dest="2"

	startTwoIgnites

	if [[ "$with_kafka" == "true" ]]; then
	  startKafkaClients "${user_ignite_properties_path1-}" "${user_ignite_properties_path2-}" \
	                                                                          "${cluster_num_src-}" "${cluster_num_dest-}"
	else
	  startIgniteClients "${user_ignite_properties_path1-}" "${cluster_num_src-}"
	fi

	getProcessInfo

	wait
}

#
# Starts Active-Active CDC
#
startActiveActive() {
	infoMsg "Starting Active-Active replication"

	mkdir -p "$IGNITE_LOG_DIR"

	local cluster_num_src="1"
	local cluster_num_dest="2"

	startTwoIgnites

	if [[ "$with_kafka" == true ]]; then
		startKafkaClients "${user_ignite_properties_path1-}" "${user_ignite_properties_path2-}" \
		                                                                        "${cluster_num_src-}" "${cluster_num_dest-}"
		startKafkaClients "${user_ignite_properties_path2-}" "${user_ignite_properties_path1-}" \
		                                                                        "${cluster_num_dest-}" "${cluster_num_src-}"
	else
		startIgniteClients "${user_ignite_properties_path1-}" "${cluster_num_src-}"
		startIgniteClients "${user_ignite_properties_path2-}" "${cluster_num_dest-}"
	fi

	getProcessInfo

	wait
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

	local result1=$(echo $json1 | grep -oP '(?<=response":)[^,]+(?=,)'\|'(?<=response":)[^\}]+' | grep -o '[0-9]*')
	local result2=$(echo $json2 | grep -oP '(?<=response":)[^,]+(?=,)'\|'(?<=response":)[^\}]+' | grep -o '[0-9]*')

	if [[ $result1 == "" ]]; then
	  value1="-"; version1="-";
	else
		value1=$(echo $json1 | grep -oP '(?<=val1":)[^,]+(?=,)'\|'(?<=val1":)[^\}]+' | grep -o '[0-9]*')
		version1=$(echo $json1 | grep -oP '(?<=val2":)[^,]+(?=,)'\|'(?<=val2":)[^\}]+' | grep -o '[0-9]*')
	fi

	if [[ $result2 == "" ]]; then
	  value2="-"; version2="-";
	else
		value2=$(echo $json2 | grep -oP '(?<=val1":)[^,]+(?=,)'\|'(?<=val1":)[^\}]+' | grep -o '[0-9]*')
		version2=$(echo $json2 | grep -oP '(?<=val2":)[^,]+(?=,)'\|'(?<=val2":)[^\}]+' | grep -o '[0-9]*')
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

	local result=""

	if [[ "$cluster" == "1" ]]; then
	  result=$(curl -s 'http://localhost:8080/ignite?cmd=put&key='$key'&val=%7B%22val1%22%3A'$value'%2C%22val2%22%3A'$version'%7D&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X POST -H 'Content-Type: application/x-www-form-urlencoded' | grep -oP '(?<=response":)[^,]+(?=,)'\|'(?<=response":)[^\}]+')
	else
	  result=$(curl -s 'http://localhost:8081/ignite?cmd=put&key='$key'&val=%7B%22val1%22%3A'$value'%2C%22val2%22%3A'$version'%7D&cacheName=terminator&keyType=int&valueType=IgniteBiTuple' -X POST -H 'Content-Type: application/x-www-form-urlencoded' | grep -oP '(?<=response":)[^,]+(?=,)'\|'(?<=response":)[^\}]+')
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
			user_ignite_properties_path=${2-}
			checkMissing "${script_param-}" "ignitePropertiesPath" "${user_ignite_properties_path-}"
			startIgnite
			;;
		-c | --cdc-client)
			checkClientParams "$@"
			startCDCClient
			;;
		--active-passive)
			checkParams "$@"
			startActivePassive
			;;
		--active-active)
			checkParams "$@"
			startActiveActive
			;;
		--check-cdc)
			checkEntriesParams "$@"
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