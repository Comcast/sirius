
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

  val dstr02x = as.actorFor("akka://sirius-system@dstr02x-dxc1.cimops.net:2552/user/sirius")
  val dstr03x = as.actorFor("akka://sirius-system@dstr03x-dxc1.cimops.net:2552/user/sirius")
  val dstr04x = as.actorFor("akka://sirius-system@dstr04x-dxc1.cimops.net:2552/user/sirius")

  val nodes = Set(dstr02x, dstr03x, dstr04x)

  def shutdown() {
    as.shutdown()
  }
}

val devTool = new DevTool

println()
println("Actors are now created for each dev-cluster node:")
println("  devTool.dstr02x, devTool.dstr03x, devTool.dstr04x")
println()
println("Messages can be sent to the actors like this:")
println("  devTool.dstr02x ! RequestLogFromRemote(devTool.dstr03x, new BoundedLogRange(1,2))")
println()
println("When finished, please execute devTool.shutdown() to save the environment")
println()
