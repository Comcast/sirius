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
import com.comcast.xfinity.sirius.api._
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.writeaheadlog._

class NoopRequestHandler extends RequestHandler {
  def handleGet(key: String) = SiriusResult.none
  def handlePut(key: String, body: Array[Byte]) = SiriusResult.none
  def handleDelete(key: String) = SiriusResult.none
}

def createSirius(logLocation: String = "/tmp/uberlog",
                 clusterConfig: String = "/tmp/cluster-config",
                 usePaxos: Boolean = true,
                 port: Int = 2552,
                 externAkkaConfig: String = "akka-overrides.conf") = {
  val cfg = new SiriusConfiguration
  cfg.setProp(SiriusConfiguration.LOG_LOCATION, logLocation)
  cfg.setProp(SiriusConfiguration.CLUSTER_CONFIG, clusterConfig)
  cfg.setProp(SiriusConfiguration.PORT, port)
  cfg.setProp(SiriusConfiguration.AKKA_EXTERN_CONFIG, externAkkaConfig)
  SiriusFactory.createInstance(new NoopRequestHandler, cfg)
}


println()
println("Most of the Sirius classes have been imported, nonetheless we may have missed some...")
println("As a convenience, some akka classes have been imported as well")
println()

