# Ignite Gatling Plugin Examples

This project contains several examples of Ignite Gatling simulations written
with the Ignite Gatling Plugin. 

It uses the [Gatling Maven Plugin](https://github.com/gatling/gatling-maven-plugin)
which is a currently recommended way to package and maintain Gatling simulations. 

## Simulations included
 
* ignite.DslPutAsyncThinPool
* ignite.DslPutGetThinTx
* ignite.LambdaPutBinarySelect

## Run simulations

Before running any simulation included here the Ignite cluster should be started manually. At least one server node should run on localhost.

### Run from command line

Individual simulation can be run by maven. Its name should be passed as a *gatling.simulationClass* system property.

For example

```bash
./mvnw gatling:test -Dgatling.simulationClass=ignite.LambdaPutBinarySelect
```

### Run from the IDE

Simulations may also be started from within the IDE:
 
- Open pom.xml as a project in IDE.
- Start the *GatlingRunner* object.
- Choose index of the simulation from the printed list entering it from keyboard and press Enter.
- Enter the optional run description.

After the simulation finish it would print link to the generated HTML report. 
