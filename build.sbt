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

version := "1.2.5"

scalaVersion := "2.10.2"

//bintray seems to be more reliable and much faster than maven central
resolvers += "Bintray" at "http://jcenter.bintray.com"

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
crossPaths := false

// compiler options
javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions ++= Seq("-deprecation", "-unchecked")

scalacOptions in (Compile,doc) ++= Seq("-doc-footer",
  "Copyright 2013-2014 Comcast Cable Communications Management, LLC", "-doc-title", "Sirius")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", "src/main/resources/overview.txt")

parallelExecution := false

// allows us to pull deps from pom file
externalPom()
