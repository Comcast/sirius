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
import java.lang.{Runtime => JRuntime}

logLevel := Level.Debug

name := "sirius"
version := "2.2.1"
scalaVersion := "2.12.8"
organization := "com.comcast"

libraryDependencies ++= {
  val akkaV = "2.4.20"

  Seq(
    "com.typesafe.akka"             %% "akka-actor"                     % akkaV,
    "com.typesafe.akka"             %% "akka-remote"                    % akkaV,
    "com.typesafe.akka"             %% "akka-slf4j"                     % akkaV,
    "com.typesafe.akka"             %% "akka-agent"                     % akkaV,
    "org.slf4j"                     %  "slf4j-api"                      % "1.7.7",
    "com.github.pathikrit"          %% "better-files"                   % "3.8.0",
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


// POM settings for Sonatype
organization := "com.comcast"
homepage := Some(url("https://github.com/Comcast/sirius"))
scmInfo := Some(ScmInfo(url("https://github.com/Comcast/sirius"), "git@github.com:Comcast/sirius.git"))
developers := List(Developer("jryan128",
  "Jonathan Ryan",
  "jonathan_ryan@comcast.com",
  url("https://github.com/jryan128")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
publishMavenStyle := true

usePgpKeyHex("9787EE9D6E7FB77E24EAEF0A0F75392379B78332")

publishTo := sonatypePublishToBundle.value

pomIncludeRepository := { _ => false }

publishArtifact in Test := false

testOptions in Test += Tests.Argument("-oD")

scoverage.ScoverageKeys.coverageMinimum := 75

scoverage.ScoverageKeys.coverageFailOnMinimum := true

scoverage.ScoverageKeys.coverageHighlighting := true

lazy val makeStandalone = taskKey[Unit]("Creates the sirius-standalone package")
lazy val root = (project in file("."))
        .withId("sirius")
        .settings(
          makeStandalone := {
            val packageDir = file("target/sirius-standalone")
            val libDir = packageDir / "lib"
            val binDir = packageDir / "bin"

            // Move artifact and dependencies into lib directory in test package
            val compilePackageBin = (packageBin in Compile).value
            val compileDependencyClasspath = (dependencyClasspath in Compile).value
            val files = compilePackageBin +: (for (dep <- compileDependencyClasspath) yield dep.data)
            for (file <- files) {
              IO.copyFile(file, libDir / file.getName)
            }

            // Move startup scripts into bin directory in test package
            IO.copyDirectory(file(".") / "src/main/bin", binDir)
            (binDir ** "*.sh").getPaths.foreach(
              (binFile) =>
                JRuntime.getRuntime.exec("chmod 755 " + binFile)
            )
            JRuntime.getRuntime.exec("chmod 755 " + (binDir / "waltool").getPath)
            JRuntime.getRuntime.exec("chmod 755 " + (binDir / "nodetool").getPath)

            JRuntime.getRuntime.exec("tar -C target -czf target/sirius-standalone.tgz sirius-standalone")
          }
        )
