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

versionScheme := Some("semver-spec")
version := "2.5.0"
ThisBuild / tlBaseVersion := "2.5"

scalaVersion := "2.13.6"
crossScalaVersions := Seq("2.11.12", "2.12.14", "2.13.6")

ThisBuild / organizationName := "Comcast Cable Communications Management, LLC"
ThisBuild / startYear := Some(2012)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("8"), JavaSpec.temurin("11"), JavaSpec.temurin("17"))
ThisBuild / githubWorkflowScalaVersions := crossScalaVersions.value
ThisBuild / tlCiMimaBinaryIssueCheck := false
ThisBuild / tlMimaPreviousVersions := Set.empty
ThisBuild / mimaFailOnNoPrevious := false

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

libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, major)) if major <= 12 =>
      Seq()
    case _ =>
      Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
  }
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
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-unchecked", "-Xlint")

Compile / doc / scalacOptions ++= Seq(
  "-doc-footer", "Copyright 2013-2014 Comcast Cable Communications Management, LLC",
  "-doc-title", "Sirius",
  "-doc-root-content", "src/main/resources/overview.txt")

parallelExecution := false


// POM settings for Sonatype
organization := "com.comcast"
homepage := Some(url("https://github.com/Comcast/sirius"))
scmInfo := Some(ScmInfo(url("https://github.com/Comcast/sirius"), "git@github.com:Comcast/sirius.git"))
developers := List(Developer("jryan128", "Jonathan Ryan", "jonathan_ryan@comcast.com", url("https://github.com/jryan128")),
  Developer("HaloFour", "Justin Spindler", "justin_spindler@comcast.com", url("https://github.com/HaloFour"))
)
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
publishMavenStyle := true
usePgpKeyHex("F61518F8742AE8EB2E3CF94BCAC601E88B813144")

val username = sys.env.getOrElse("ARTIFACTORY_USER", null)
val password = sys.env.getOrElse("ARTIFACTORY_PASSWORD", null)
credentials += Credentials("Artifactory Realm", "artifactory.comcast.com", username, password)
sonatypeSessionName := s"[sbt-sonatype] ${name.value}-${scalaBinaryVersion.value}-${version.value}"
publishTo := Some("Artifactory Realm" at "https://artifactory.comcast.com/artifactory/xvp-libs-releases")
pomIncludeRepository := { _ => false }
publishConfiguration := publishConfiguration.value.withOverwrite(true)

Test / publishArtifact := false

Test / testOptions += Tests.Argument("-oD")

scoverage.ScoverageKeys.coverageMinimumStmtTotal := 75

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
            val compilePackageBin = (Compile / packageBin).value
            val compileDependencyClasspath = (Compile / dependencyClasspath).value
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
