/*
 *  Copyright 2012-2013 Comcast Cable Communications Management, LLC
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

class DevTool {
  import akka.actor.ActorSystem
  import com.typesafe.config.ConfigFactory


  private val config = ConfigFactory.parseString("""
    akka {
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        transport = "akka.remote.netty.NettyRemoteTransport"
        netty {
          hostname = "127.0.0.1"
          port = 2552
        }
      }
    }
  """)

  val as = ActorSystem("DevTool", ConfigFactory.load(config))

  val dstr02x = as.actorSelection("akka://sirius-system@dstr02x-dxc1.cimops.net:2552/user/sirius")
  val dstr03x = as.actorSelection("akka://sirius-system@dstr03x-dxc1.cimops.net:2552/user/sirius")
  val dstr04x = as.actorSelection("akka://sirius-system@dstr04x-dxc1.cimops.net:2552/user/sirius")
  val pal04x  = as.actorSelection("akka://sirius-system@pal04x-dxc1.cimops.net:2552/user/sirius")

  val nodes = Set(dstr02x, dstr03x, dstr04x, pal04x)

  def shutdown() {
    as.shutdown()
  }
}

val devTool = new DevTool

println()
println("Actors are now created for each dev-cluster node:")
println("  devTool.dstr02x, devTool.dstr03x, devTool.dstr04x, devTool.pal04x")
println()
println("Messages can be sent to the actors like this:")
println("  devTool.dstr02x ! RequestLogFromRemote(devTool.dstr03x, new BoundedLogRange(1,2))")
println()
println("When finished, please execute devTool.shutdown() to save the environment")
println()
