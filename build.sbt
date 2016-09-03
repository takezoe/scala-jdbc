name := "scala-jdbc"

organization := "com.github.takezoe"

version := "1.0.3-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.github.jsqlparser" % "jsqlparser" % "0.9.6",
  "org.scalamacros" %% "resetallattrs"  % "1.0.0",
  "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else                             Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation")

publishArtifact in Test := false

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
