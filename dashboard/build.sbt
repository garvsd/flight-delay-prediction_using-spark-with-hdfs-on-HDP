name := "insightedge-python-demo"
version := "1.0"
scalaVersion := "2.10.4"

val kafkaLibs = Seq(
  "org.apache.kafka" %% "kafka" % "0.8.2.2"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

def insightEdgeLibs(scope: String) = Seq(
  "org.gigaspaces.insightedge" % "insightedge-core" % "1.0.0" % scope exclude("javax.jms", "jms"),
  "org.gigaspaces.insightedge" % "insightedge-scala" % "1.0.0" % scope exclude("javax.jms", "jms")
)
val openspaceResolvers = Seq(
  Resolver.mavenLocal,
  "Openspaces Maven Repository" at "http://maven-repository.openspaces.org"
)

lazy val root = project.in(file("."))

lazy val web = project
  .enablePlugins(PlayScala)
  .settings(resolvers ++= openspaceResolvers)
  .settings(libraryDependencies ++= kafkaLibs)
  .settings(libraryDependencies ++= insightEdgeLibs("compile"))
