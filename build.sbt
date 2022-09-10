name := "scala-jdbc"

organization := "com.github.takezoe"

version := "1.0.6"

scalaVersion := "2.13.8"

crossScalaVersions := Seq("2.11.12", "2.12.16", "2.13.8", "3.2.0")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
  "com.h2database" % "h2" % "1.4.192" % "test"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation", "-feature")

//unmanagedClasspath in Compile += baseDirectory.value / "src" / "main" / "resources"

Test / publishArtifact := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/takezoe/scala-jdbc</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/takezoe/scala-jdbc</url>
    <connection>scm:git:https://github.com/takezoe/scala-jdbc.git</connection>
  </scm>
  <developers>
    <developer>
      <id>takezoe</id>
      <name>Naoki Takezoe</name>
      <email>takezoe_at_gmail.com</email>
      <timezone>+9</timezone>
    </developer>
  </developers>
)
