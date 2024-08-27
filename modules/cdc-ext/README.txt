Apache Ignite Change Data Capture Module
------------------------

Apache Ignite CDC is a data processing pattern used to asynchronously receive entries that have been changed on the local node so that action can be taken using the changed entry.

This module provides the clients with a simple CDC implementations strategies for inter-cluster communication. Active-Passive and Active-Active replication regimes can be established with different CDC clients. Each such client should be started up for each node participating in CDC.

For that move 'ignite-cdc' folder to 'libs' folder before running 'ignite-cdc.{sh|bat}' or 'kafka-to-ignite.{sh|bat}' scripts. The content of the module folder will be added to classpath in this case.

You can build the binary for the module from the source code by using:

mvn clean install -f modules/cdc-ext -Pcheckstyle -DskipTests