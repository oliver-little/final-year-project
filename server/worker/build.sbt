val scala3Version = "3.2.1"

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
      "com.datastax.oss" % "java-driver-core" % "4.14.0"
    ),

    dockerExposedPorts := Seq(50051)
  )
  .dependsOn(core)

// Protobufs are compiled in core
lazy val core = RootProject(file("../core"))

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)