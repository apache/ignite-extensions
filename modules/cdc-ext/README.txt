Apache Ignite Change Data Capture Module
------------------------

Apache Ignite CDC is a data processing pattern used to asynchronously receive entries that have been changed on the local node so that action can be taken using the changed entry.

This module provides the clients with a simple CDC implementations strategies for inter-cluster communication. Active-Passive and Active-Active replication regimes can be established with different CDC clients. Each such client should be started up for each node participating in CDC.

==== Installation

. Build `cdc-ext` module with maven:
+
```console
  $~/src/ignite-extensions/> mvn clean package -DskipTests
  $~/src/ignite-extensions/> ls modules/cdc-ext/target | grep zip
ignite-cdc-ext.zip
```

. Unpack `ignite-cdc-ext.zip` archive to `$IGNITE_HOME` folder.

Or you can build the binary for the module from the source code by using the following command from the repository root:
+
```console
mvn clean install -f modules/cdc-ext -Pcheckstyle,extension-release,skip-docs -DskipTests
```
The resulting binary will be located under 'target' directory. Unpack it to ignite binary root to enable CDC.

Now, you have additional binary `$IGNITE_HOME/bin/kafka-to-ignite.sh` and `$IGNITE_HOME/libs/optional/ignite-cdc-ext` module.

Use Apache Ignite documentation to explore CDC capabilities.
