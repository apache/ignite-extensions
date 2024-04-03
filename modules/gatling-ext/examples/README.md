# Ignite Gatling Plugin Examples

This project contains several examples of Ignite Gatling simulations written
with the Ignite Gatling Plugin. 

It uses the [Gatling Maven Plugin](https://github.com/gatling/gatling-maven-plugin)
which is a currently recommended way to package and maintain Gatling simulations. 

Note, that these example simulations start the Ignite server node by themselves. This is done for simplicity.

In the real testing conditions the Ignite server nodes forming the cluster under the test would be started in some
other way outside the simulation class.

## Simulations included
 
* ignite.DslPutAsyncThinPool
* ignite.DslPutGetThinTx
* ignite.LambdaPutBinarySelect

## Run simulation from command line

Individual simulation can be run by maven. Its name should be passed as a *gatling.simulationClass* system property.

```bash
cd modules/gatling-ext
mvn gatling:test -Dgatling.simulationClass=ignite.LambdaPutBinarySelect
```

## Run simulation from the IDE

Simulations may also be started from within the IDE:
 
- Open `modules/gatling-ext/pom.xml` as a project in IDE.
- Start the *GatlingRunner* object.
- Choose index of the simulation from the printed list entering it from keyboard and press Enter.
- Enter the optional run description.

After the simulation finish it would print link to the generated HTML report. 
