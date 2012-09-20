import com.comcast.xfinity.sirius.api._
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.impl.persistence._
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
  cfg.setProp(SiriusConfiguration.USE_PAXOS, usePaxos)
  cfg.setProp(SiriusConfiguration.PORT, port)
  cfg.setProp(SiriusConfiguration.AKKA_EXTERN_CONFIG, externAkkaConfig)
  SiriusFactory.createInstance(new NoopRequestHandler, cfg)
}


println()
println("Most of the Sirius classes have been imported, nonetheless we may have missed some...")
println("As a convenience, some akka classes have been imported as well")
println()

