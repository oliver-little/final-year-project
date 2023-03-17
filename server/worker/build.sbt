val scala3Version = "3.2.1"
val AkkaVersion = "2.7.0"

Universal / javaOptions ++= Seq(
  "-J-XX:MaxRAMPercentage=97.5",
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "worker",
    version := "latest",
    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      // ScalaTest
      "org.scalactic" %% "scalactic" % "3.2.14",
      "org.scalatest" %% "scalatest" % "3.2.14" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      // gRPC
      "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
      // Cassandra Driver - could also use Phantom or Quill
      "com.datastax.oss" % "java-driver-core" % "4.14.0",
      // Akka actors + test dependencies
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
      // SLF4J
      "org.slf4j" % "slf4j-api" % "2.0.6",
      "org.slf4j" % "slf4j-simple" % "2.0.6"
    ),

    dockerExposedPorts := Seq(50051)
  )
  .dependsOn(core)

// Protobufs are compiled in core
lazy val core = RootProject(file("../core"))

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)