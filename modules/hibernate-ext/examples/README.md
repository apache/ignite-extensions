# Apache Ignite Examples

This module contains examples of how to run [Apache Ignite](ignite.apache.org) and [Apache Ignite](ignite.apache.org) with 3rd party components.

Instructions on how to start examples can be found in [README.txt](README.txt).

## Running examples

These examples require JDK 17 or later. Ignite accesses JDK internals; see the required JVM options in [How to run Ignite](https://ignite.apache.org/docs/latest/setup#running-ignite-with-java-17-or-later).

To run examples from an IDE, add the JVM options listed there to your run configuration.

For example, for IntelliJ IDEA it is possible to use Application Templates.

Use 'Run' -> 'Edit Configuration' menu.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vQFgjhrPsLPUmic8CA_s1YpjVwA2vQITxNsLrAKOecZxIQEZSb1Ps2XKh0QEn8z9vtYiUofnGek_cag/pub?w=960&h=720"/>

## Contributing to Examples

*Notice* When updating classpath of examples and in case any modifications required in [pom.xml](pom.xml)
please make sure that corresponding changes were applied to
 * [pom-standalone.xml](pom-standalone.xml),
 * [pom-standalone-lgpl.xml](pom-standalone-lgpl.xml).
 
 These pom files are finalized during release and placed to the `examples` folder with these examples code.
