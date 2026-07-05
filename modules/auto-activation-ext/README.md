Apache Ignite Auto Activation Plugin
------------------------------------
Apache Ignite Auto Activation plugin enables cluster activation at startup, subject to configured conditions.

Plugin skip cluster activation in any of next cases:

- Cluster state is ACTIVE
- Cluster baseline is not empty

Depending on how you use Ignite, you can an extension using one of the following methods:

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
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

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
            <version>${ignite-auto-activation-ext.version}</version>
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
or
```
<bean id="condition" class="opt.apache.ignite.activation.ActivateByNodeAttribute">
    <constructor-arg name="attributeName" value="ATTR"/>
    <constructor-arg name="requiredValues">
        <util:set>
            <value>server-0</value>
            <value>server-1</value>
        </util:set>
    </constructor-arg>
</bean>
```
