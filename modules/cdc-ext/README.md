Apache Ignite Change Data Capture Module
------------------------

Apache Ignite CDC is a data processing pattern used to asynchronously receive entries that have been changed on the local node so that action can be taken using the changed entry.

This module provides the clients with simple CDC implementations strategies for inter-cluster communication. Active-Passive and Active-Active replication strategies can be established with different CDC clients. Each such client should be started up for each node participating in CDC.

==== Installation

. Build `cdc-ext` module with maven:
+
```console
  $~/src/ignite-extensions/> mvn clean install -f modules/cdc-ext -Pcheckstyle,extension-release,skip-docs -DskipTests
  $~/src/ignite-extensions/> ls modules/cdc-ext/target | grep zip
ignite-cdc-ext-bin.zip
```

. The resulting binary will be located under 'target' directory. Unpack `ignite-cdc-ext-bin.zip` archive to `$IGNITE_HOME` folder to enable CDC.

For Linux/Macos you can use in the ignite root with cdc binary:
+
```console
$ unzip ignite-cdc-ext-bin.zip
$ cp -r ignite-cdc-ext/* .
```

Now, you have additional binary `$IGNITE_HOME/bin/kafka-to-ignite.sh`, `$IGNITE_HOME/libs/optional/ignite-cdc-ext` module and configuration examples under `$IGNITE_HOME/examples/config/cdc-start-up`

To run the examples, use the binary `$IGNITE_HOME/examples/cdc-start-up/cdc-start-up.sh`

Use Apache Ignite documentation to explore CDC capabilities.
