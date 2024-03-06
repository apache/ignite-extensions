# Ignite Gatling Plugin

## Introduction

Plugin adding the Apache Ignite support to Gatling(3.7.x)

## Supported Ignite operations

Basic Ignite operations are supported via the DSL as:

```scala
val scn = scenario("transaction")
      .feed(feeder)
      .ignite(
          tx concurrency PESSIMISTIC isolation READ_COMMITTED timeout 3000 size 2 run (
              get[Int, Int]("cache-1", s"#{$key}"),

              put[Int, Int]("cache-2", s"#{$key}", s"#{$value}"),

              commit
          )
      )

val protocol = igniteProtocol
    .cfg(Ignition.startClient(
      new ClientConfiguration().setAddresses("localhost:10800"))
    )

setUp(scn.inject(constantUsersPerSec(10) during 10.seconds))
    .protocols(protocol)
```

The following operations are supported by the DSL:

* ignite
  * start client / close client
  * start node / stop node
  * createCache
* cache
  * get / put / remove
  * getAndPut / getAndRemove
  * getAll / putAll / removeAll
  * sql query
  * invoke / invokeAll
  * lock / unlock
* transaction
  * txStart
  * commit
  * rollback


Any Ignite Java API function not covered by the DSL may be used as follows if needed:

```scala
val scn = scenario("compute")
    .feed(feeder)
    .exec { session =>
        session.igniteApi match {
            case IgniteNodeApi(ignite) => ignite.compute().execute("task", 1)

            case IgniteThinApi(igniteClient) => igniteClient.compute().execute("task", 1)
        }

        session
    }
```

## Example

Basic Ignite Gatling simulation is [here](src/test/scala/org/apache/ignite/gatling/examples)
