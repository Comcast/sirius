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

name := "sirius"

version := "2.3.0"

scalaVersion := "2.13.6"
crossScalaVersions := Seq("2.11.12", "2.12.14", "2.13.6") // NOTE: keep sync'd with .travis.yml

organization := "com.comcast"

//bintray seems to be more reliable and much faster than maven central
resolvers += Resolver.bintrayIvyRepo("sbt", "sbt-plugin-releases")
resolvers += "Typesafe Public Repo" at "https://repo.typesafe.com/typesafe/releases"
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= {
  val akkaV = "2.5.32"
  val scalaTestV = "3.2.9"

  Seq(
    "com.typesafe.akka"             %% "akka-actor"                     % akkaV,
    "com.typesafe.akka"             %% "akka-remote"                    % akkaV,
    "com.typesafe.akka"             %% "akka-slf4j"                     % akkaV,
    "com.typesafe.akka"             %% "akka-agent"                     % akkaV,
    "org.slf4j"                     %  "slf4j-api"                      % "1.7.30",
    "com.github.pathikrit"          %% "better-files"                   % "3.9.1",
    "org.scalatest"                 %% "scalatest"                      % scalaTestV  % Test,
    "org.scalatest"                 %% "scalatest-funspec"              % scalaTestV  % Test,
    "org.scalatestplus"             %% "junit-4-13"                     % "3.2.2.0"   % Test,
    "org.scalatestplus"             %% "mockito-3-3"                    % "3.2.2.0"   % Test,
    "junit"                         %  "junit"                          % "4.13"      % Test,
    "org.slf4j"                     %  "slf4j-log4j12"                  % "1.7.30"    % Test,
    "log4j"                         %  "log4j"                          % "1.2.17"    % Test,
    "com.typesafe.akka"             %% "akka-testkit"                   % akkaV       % Test
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

sonatypeSessionName := s"[sbt-sonatype] ${name.value}-${scalaBinaryVersion.value}-${version.value}"

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
