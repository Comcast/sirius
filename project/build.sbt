/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.lang.{Runtime => JRuntime}

val makeStandalone = TaskKey[Unit]("make-standalone",
    "Creates the sirius-standalone package")
val packageDir = file("target/sirius-standalone")
val libDir = packageDir / "lib"
val binDir = packageDir / "bin"

lazy val root = (project in file("."))
        .withId("sirius")
        .settings(
            makeStandalone := {
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
