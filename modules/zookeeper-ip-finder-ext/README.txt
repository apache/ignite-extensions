Apache Ignite ZooKeeper IpFinder Module
------------------------------

Apache Ignite ZooKeeper IpFinder module provides a TCP Discovery IP Finder that uses a ZooKeeper
directory to locate other Ignite nodes to connect to.

Importing Apache Ignite ZooKeeper IpFinder Module In Maven Project
---------------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the ZooKeeper
module dependency like this (replace '${ignite-zookeeper-ip-finder.version}' with actual Ignite version you
are interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-zookeeper-ip-finder-ext</artifactId>
            <version>${ignite-zookeeper-ip-finder.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
