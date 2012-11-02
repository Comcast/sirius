name := "sirius"

version := "1.0.1-SNAPSHOT"

scalaVersion := "2.9.2"

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

// look in local maven repo
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

// look in cim repo
resolvers += "Cim Nexus Public Mirror" at "http://repo.dev.cim.comcast.net/nexus/content/groups/public"

// TODO: figure out how to exclude maven central

// allows us to pull deps from pom file
externalPom()
