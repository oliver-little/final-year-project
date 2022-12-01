val scala3Version = "3.2.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "orchestrator",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      // ScalaTest
      "org.scalactic" %% "scalactic" % "3.2.14",
      "org.scalatest" %% "scalatest" % "3.2.14" % "test",
      // gRPC
      "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
      // Cassandra Driver - could also use Phantom or Quill
      "com.datastax.oss" % "java-driver-core" % "4.14.0"
    )
  )
  .dependsOn(core)

// Protobufs are compiled in core
lazy val core = RootProject(file("../core"))
