Apache Ignite Auto Activation Plugin
------------------------------------
Apache Ignite Auto Activation plugin enables cluster activation at startup, subject to configured conditions.

The plugin skips cluster activation in the following cases:

- Cluster state is either ACTIVE or ACTIVE_READ_ONLY, log message:
```text
 [DateTime][INFO][main][AutoActivationPluginProvider] Auto activation skipped - cluster already activated
```
- Cluster baseline topology is not empty, log message:
```text
 [DateTime][INFO][main][AutoActivationPluginProvider] Auto activation skipped - baseline is not empty
```
- The baseline topology does not include all nodes listed for activation. A message containing the consistentIds of the missing nodes will be written to ignite.log:
```text
 [DateTime][INFO][main][AutoActivationPluginProvider] Auto activation skipped - activation condition not meet (by consistent ID). Missing nodes [<nideConssistentId1>, <nideConssistentId2>, ...]
```
- The node attributes in the topology do not contain the full list of values specified in the cluster's auto-activation settings. The log message will indicate the attribute name and list the missing values as follows:
```text
[DateTime][INFO][main][AutoActivationPluginProvider] Auto activation skipped - activation condition not meet (by node attribute). Attribute: <attributeName>, Missing values [<value1>, <value1>, ...]
```
- The required nodes list for cluster activation contains any client node. In this case, the node will simply not participate in cluster activation, and the log message will be identical to that of a missing node.

Depending on how you use Ignite, you can implement an extension using one of the following methods:

- If you use the binary distribution, move the libs/{module-dir} to the 'libs' directory of the Ignite distribution before starting the node.
- Add libraries from libs/{module-dir} to the classpath of your application.
- Add a module as a Maven dependency to your project.


Building Module And Running Tests
---------------------------------

To build and run Auto Activation extension use the command below:

mvn clean package -pl modules/auto-activation-ext


Importing Auto Activation Plugin In Maven Project
-------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add Auto Activation Plugin module
dependency like this:

```xml

<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <artifactId>your.project</artifactId>
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-auto-activation-ext</artifactId>
            <version>1.0.0-SNAPSHOT</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
```

Usage
-----------------------------------

To enable cluster auto activation add next properties to your ignite-server.xml configurations
```
<bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration">
    <property name="pluginProviders">
        <bean class="opt.apache.ignite.activation.AutoActivationPluginProvider">
            <constructor-arg name="condition" ref="condition" />
        </bean>
    </property>
</bean>
```
where "condition" can be one of the following beans:
```
<bean id="condition" class="opt.apache.ignite.activation.ActivateByConsistentID">
    <constructor-arg name="requiredNodes">
        <util:set>
            <value>server-0</value>
            <value>server-1</value>
        </util:set>
    </constructor-arg>
</bean>
```
where `server-0` and `server-1` are consistent ID's of required server nodes in the activated cluster

or
```
<bean id="condition" class="opt.apache.ignite.activation.ActivateByNodeAttribute">
    <constructor-arg name="attributeName" value="ATTR"/>
    <constructor-arg name="requiredValues">
        <util:set>
            <value>attribute-0</value>
            <value>attribute-1</value>
        </util:set>
    </constructor-arg>
</bean>
```
where `attribute-0` and `attribute-1` are values of user-defined attribute `ATTR` that will be used to choose server nodes for cluster auto activation.