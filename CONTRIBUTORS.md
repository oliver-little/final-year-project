# Dependencies

There are a number of dependencies without which this project would not have been possible to complete. Most can be found in the build.sbt file for scala dependencies, and requirements.txt for python dependencies.

However, a list of key dependencies is included below:

## Scala

DataStax Java Driver: [GitHub](https://github.com/datastax/java-driver)

scalapb (and gRPC): [GitHub](https://scalapb.github.io/)

Akka Actors: [GitHub](https://github.com/akka/akka)

Spark Size Estimator:
- Note: this is a single file (not a standalone project), and can be found in the repository [here.](server/core/src/main/scala/org/oliverlittle/clusterprocess/dependency/SizeEstimator.scala)
- The file was sourced and adapted for Scala 3 from the following locations:
    - [Spark Project](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/SizeEstimator.scala)
    - [Scala 2 adaptation](https://github.com/phatak-dev/java-sizeof/blob/master/src/main/scala/com/madhukaraphatak/sizeof/SizeEstimator.scala)
- The full Spark repository can be found here: [GitHub](https://github.com/apache/spark)
- The LICENSE for this file (Apache 2.0) can be found here: [LICENSE](https://github.com/apache/spark/blob/master/LICENSE)
- The NOTICE for this file can be found here: [NOTICE](https://github.com/apache/spark/blob/master/NOTICE)

scalatest: [scalatest.org](https://www.scalatest.org/)

## Python

DataStax Python Driver: [GitHub]()

Pandas: [pandas.pydata.org](https://pandas.pydata.org)

gRPC: (GitHub)[https://github.com/grpc/grpc]

pytest: [pytest.org](https://docs.pytest.org/en/7.2.x/)