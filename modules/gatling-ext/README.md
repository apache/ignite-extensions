# Ignite Gatling Plugin

## Introduction

Plugin allowing to run load tests against the [Apache Ignite](https://ignite.apache.org/) clusters using [Gatling](https://gatling.io/) (3.10.x)

Plugin is currently available for Scala.

## Usage

You may add plugin as dependency in project with your tests. Write this to your POM file: 

```xml
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-gatling-plugin</artifactId>
            <version>${version}</version>
        </dependency>
```

## Build

To build everything and run all tests and examples from command line do the following:

```
cd ignite-extensions
mvn clean package -f modules/gatling-ext
```

## Examples

See [here](examples) the example project based on the [Gatling Maven Plugin](https://github.com/gatling/gatling-maven-plugin).

## Features

### Ignite Operations DSL

The most popular Ignite operations are supported via the DSL which allows to define the test scenario 
in a way like below:

```scala
val scn = scenario("PutGetTx")
    .feed(feeder)
    .ignite(
        // Execute in transaction.
        tx concurrency PESSIMISTIC isolation REPEATABLE_READ timeout 3000 size 2 run (
            put[Int, Int]("cache", "#{key}", "#{value}") as "txPut",

            get[Int, Int]("cache", "#{key}").check(
                entries[Int, Int].transform(_.value).is("#{value}")
            ) as "txGet",

            sql("City", "SELECT * FROM City WHERE Id=?")
                .args("#{Id}")
                .check(resultSet.count.is(1)) as "txSelect",

            commit as "txCommit"
        ) as "transaction"
    )
```

If the operations DSL is used each individual operation (like transaction start, put, get and commit in the above example)
would be measured and represented separately in the final HTML report.  Besides, the gatling group will be created for the
transaction to measure it as a whole.

Names of operations to be used in report are specified via the `as` keyword.

Async operations are also supported via the `async` keyword. The limitation is that it's 
only available outside the transactions.

```scala
val scn = scenario("PutAsync")
    .feed(feeder)
    .ignite(
        put[Int, Int]("cache", "#{key}", "#{value}") as "put" async
    )
```

The following operations are supported by the DSL:

* ignite
    * start client / close client
    * getOrCreateCache
* cache
    * get / put / remove
    * getAll / putAll / removeAll
    * getAndPut / getAndRemove
    * sql query
    * invoke / invokeAll
    * lock / unlock
* transactions
    * tx (start transaction)
    * commit
    * rollback

See examples for each supported Ignite operations DSL in [unit tests](gatling-plugin/src/test/scala/org/apache/ignite/gatling).

### Ignite Lambda DSL

For more complex scenarios more generic DSL is available. It allows to implement function or lambda
accepting Ignite or IgniteClient instance and invoke arbitrary Ignite APIs and use it in Gatling scenario.

It may be implemented in Java if Scala is not desirable. 

```scala

def ignitePut = { ignite: Ignite =>
    val cache = ignite.cache("City").withKeepBinary[Int, BinaryObject]()

    val id = ThreadLocalRandom.current().nextInt()

    val value = ignite.binary().builder("City")
        .setField("Id", id)
        .setField("Name", ThreadLocalRandom.current().alphanumeric.take(20).mkString)
        .build()

    cache.put(id, value)
}

val scn: ScenarioBuilder = scenario("PutBinary")
    .feed(feeder)
    .exec { 
        ignitePut as "put" 
    }
```

Individual operations inside lambda are not measured by Gatling separately. Durations are measured
and reported for the lambda as a while.

### Ignite connection DSL

Connection to Ignite cluster is configured via the `igniteProtocol` element. Several options are supported.

```scala
// Ignite node config instance
val protocol: IgniteProtocol = igniteProtocol.igniteCfg(new IgniteConfiguration().setClientMode(true))

// Ignite node instance
val protocol: IgniteProtocol = igniteProtocol.ignite(
    Ignition.start(new IgniteConfiguration().setClientMode(true)))

// Ignite node Spring XML config path:
val protocol: IgniteProtocol = igniteProtocol.igniteCfgPath("config/default-config.xml")

// Ignite Thin Client config instance:
val protocol: IgniteProtocol = igniteProtocol.clientCfg(new ClientConfiguration())

// Ignite Thin Client instance
val protocol: IgniteProtocol = igniteProtocol.client(
    Ignition.startClient(new ClientConfiguration()))

// Ignite Thin Client Spring XML config path:
val protocol: IgniteProtocol = igniteProtocol.clientCfgPath("config/ignite-thin-config.xml")

// Some Ignite Thin Client pool implementation
val protocol: IgniteProtocol = igniteProtocol.clientPool(
    new IgniteClientPerThreadPool(new ClientConfiguration()))
```

If configs are used for protocol creation Gatling starts a single instance of client node or thin client before
the simulation start. This instance is used by all threads invoked by Gatling. In such case the started node or
client should be closed explicitly in the simulation's *after* sections as:

```scala
    after {
        protocol.close()
    }
```

If the pre-started node or client is used for protocol creation it also should be closed in the *after* section like:
```scala
    val ignite = Ignition.start(new IgniteConfiguration().setClientMode(true))

    val protocol: IgniteProtocol = igniteProtocol.ignite(ignite)

    after {
        ignite.close()
    }
```

If the thin client is used protocol may be created passing the client pool. Pool is an implementation 
of the *org.apache.ignite.gatling.protocol.IgniteClientPool* trait. Gatling requests the pool for the
thin client instance once it needs one.

There are two predefined pool implementations:
 * *IgniteClientPerThreadPool* - dedicated client is created per thread invoked by the Gatling.
 * *IgniteClientFixedSizePool* - random client from the pre-started ones is returned. 

If pool is used it should be closed in the simulation's *after* sections as:

```scala
    val pool = new IgniteClientPerThreadPool(
        new ClientConfiguration().setAddresses("localhost:10800")
    )
    
    val protocol: IgniteProtocol = igniteProtocol.clientPool(pool)

    after {
        pool.close()
    }
```
