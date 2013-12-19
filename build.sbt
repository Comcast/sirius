//
// Copyright 2012-2013 Comcast Cable Communications Management, LLC
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

version := "1.2.0-SNAPSHOT"

scalaVersion := "2.10.2"

// Set the artifact names.
artifactName := { (scalaVersion: String, module: ModuleID, artifact: Artifact) =>
  artifact.`type` match {
    case "jar" => "sirius.jar"
    case "src" => "sirius-sources.jar"
    case "doc" => "sirius-javadoc.jar"
    case _ => Artifact.artifactName(scalaVersion, module, artifact)
  }
}

// disable using the Scala version in output paths and artifacts
crossPaths := false

// compiler options
javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions ++= Seq("-deprecation", "-unchecked")

// exclude maven central
externalResolvers := Seq(
    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    "Cim Nexus Public Mirror" at "http://repo.dev.cim.comcast.net/nexus/content/groups/public"
)

// allows us to pull deps from pom file
externalPom()
