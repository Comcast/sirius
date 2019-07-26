//
// Copyright 2012-2014 Comcast Cable Communications Management, LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

name := "sirius"

version := "2.1.1-SNAPSHOT"

scalaVersion := "2.12.8"
crossScalaVersions := Seq("2.11.8", "2.12.8")

organization := "com.comcast"

//bintray seems to be more reliable and much faster than maven central
resolvers += "Bintray" at "http://jcenter.bintray.com"

resolvers += "Typesafe Public Repo" at "http://repo.typesafe.com/typesafe/releases"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= {
  val akkaV = "2.4.20"

  Seq(
    "com.typesafe.akka"             %% "akka-actor"                     % akkaV,
    "com.typesafe.akka"             %% "akka-remote"                    % akkaV,
    "com.typesafe.akka"             %% "akka-slf4j"                     % akkaV,
    "com.typesafe.akka"             %% "akka-agent"                     % akkaV,
    "org.slf4j"                     %  "slf4j-api"                      % "1.7.7",
    "com.github.pathikrit"          %% "better-files"                   % "3.7.0", 
    "org.scalatest"                 %% "scalatest"                      % "3.0.5"   % "test",
    "org.mockito"                   %  "mockito-core"                   % "1.10.19" % "test",
    "junit"                         %  "junit"                          % "4.12"    % "test",
    "org.slf4j"                     %  "slf4j-log4j12"                  % "1.7.7"   % "test",
    "log4j"                         %  "log4j"                          % "1.2.17"  % "test",
    "com.typesafe.akka"             %% "akka-testkit"                   % akkaV     % "test"
  )
}

// Set the artifact names.
artifactName := { (scalaVersion: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.`type` match {
    case "jar" => "sirius.jar"
    case "src" => "sirius-sources.jar"
    case "doc" => "sirius-javadoc.jar"
    case _ => Artifact.artifactName(scalaVersion, module, artifact)
  }
}

// disable using the Scala version in output paths and artifacts
crossPaths := true

// compiler options
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions ++= Seq("-deprecation", "-unchecked")

scalacOptions in (Compile,doc) ++= Seq("-doc-footer",
  "Copyright 2013-2014 Comcast Cable Communications Management, LLC", "-doc-title", "Sirius")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", "src/main/resources/overview.txt")

parallelExecution := false

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/Comcast/sirius</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:Comcast/sirius.git</url>
      <connection>scm:git@github.com:Comcast/sirius.git</connection>
      <developerConnection>scm:git@github.com:Comcast/sirius.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>Comcast</id>
        <name>Comcast</name>
        <email>...</email>
      </developer>
    </developers>
  )

publishArtifact in Test := false

testOptions in Test += Tests.Argument("-oD")

scoverage.ScoverageKeys.coverageMinimum := 75

scoverage.ScoverageKeys.coverageFailOnMinimum := true

scoverage.ScoverageKeys.coverageHighlighting := true
