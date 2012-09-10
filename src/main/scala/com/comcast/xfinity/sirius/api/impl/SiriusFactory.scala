package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.writeaheadlog.{SiriusLog, CachedSiriusLog}
import java.net.InetAddress
import scalax.file.Path
import java.util.{HashMap => JHashMap}
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{ActorRef, ActorSystem}
import management.ManagementFactory
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.admin.SiriusAdmin

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances
 */
object SiriusFactory extends AkkaConfig {

  /**
   * SiriusImpl factory method, takes parameters to construct a SiriusImplementation and the dependent
   * ActorSystem and return the created instance.  Calling shutdown on the produced SiriusImpl will also
   * shutdown the dependent ActorSystem.
   *
   * @param requestHandler the RequestHandler containing callbacks for manipulating the system's state
   * @param siriusConfig a SiriusConfiguration containing configuration info needed for this node.
   * @see SiriusConfiguration for info on needed config.
   *
   * @return A SiriusImpl constructed using the parameters
   */
  def createInstance(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration): SiriusImpl = {
    val uberStoreDir = siriusConfig.getProp[String](SiriusConfiguration.LOG_LOCATION) match {
      case Some(dir) => dir
      case None =>
        throw new IllegalArgumentException(SiriusConfiguration.LOG_LOCATION + " must be set on config")
    }
    val backendLog = UberStore(uberStoreDir)
    val log = CachedSiriusLog(backendLog)
    createInstance(requestHandler, siriusConfig, log)
  }

/**
   * SiriusImpl factory method, takes parameters to construct a SiriusImplementation and the dependent
   * ActorSystem and return the created instance.  Calling shutdown on the produced SiriusImpl will also
   * shutdown the dependent ActorSystem.
   *
   * @param requestHandler the RequestHandler containing callbacks for manipulating the system's state
   * @param siriusLog the persistence layer to which events should be committed to and replayed from
   *          note, this parameter may be removed in future refactorings
   * @param hostName the hostName or IP to which this instance should bind.  It is important that other
   *          Sirius instances identify this host by this name.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param port the port which this instance should bind to.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param clusterConfigPath string pointing to the location of this cluster's configuration.  This should
   *          be a file with Akka style addresses on each line indicating membership. For more information
   *          see http://doc.akka.io/docs/akka/snapshot/general/addressing.html
   * @param usePaxos should the underlying implementation use Paxos for ordering events? If true it will,
   *          if not it will use use a simple monotonically increasing counter, which is good enough
   *          as long as this instance isn't clustered
   *
   * @return A SiriusImpl constructed using the parameters
   */
  private[sirius] def createInstance(requestHandler: RequestHandler, siriusLog: SiriusLog, hostName: String, port: Int,
                   clusterConfigPath: String, usePaxos: Boolean): SiriusImpl = {

    val siriusConfig = new SiriusConfiguration
    siriusConfig.setProp(SiriusConfiguration.HOST, hostName)
    siriusConfig.setProp(SiriusConfiguration.PORT, port)
    siriusConfig.setProp(SiriusConfiguration.CLUSTER_CONFIG, clusterConfigPath)
    siriusConfig.setProp(SiriusConfiguration.USE_PAXOS, usePaxos)

    createInstance(requestHandler, siriusConfig, siriusLog)
  }

  /**
   * USE ONLY FOR TESTING HOOK WHEN YOU NEED TO MOCK OUT A LOG.
   * Real code should use the two argument factory method.
   *
   * @param requestHandler the RequestHandler containing callbacks for manipulating the system's state
   * @param siriusConfig a SiriusConfiguration containing configuration info needed for this node.
   * @see SiriusConfiguration for info on needed config.
   * @param siriusLog the persistence layer to which events should be committed to and replayed from.
   *
   * @return A SiriusImpl constructed using the parameters
   */
  private[sirius] def createInstance(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration,
                   siriusLog: SiriusLog): SiriusImpl = {

    val host = siriusConfig.getProp(SiriusConfiguration.HOST, InetAddress.getLocalHost.getHostName)
    val port = siriusConfig.getProp(SiriusConfiguration.PORT, 2552)

    implicit val actorSystem = ActorSystem(SYSTEM_NAME, createActorSystemConfig(host, port))
    val impl = new SiriusImpl(
      requestHandler,
      siriusLog,
      Path.fromString(siriusConfig.getClusterConfigPath),
      siriusConfig
    )

    // create the stuff to expose mbeans
    val admin = createAdmin(host, port, impl.supervisor)
    admin.registerMbeans()

    // need to shut down the actor system and unregister the mbeans when sirius is done
    impl.onShutdown({
      actorSystem.shutdown()
      actorSystem.awaitTermination()
      admin.unregisterMbeans()
    })

    impl
  }

  private def createHostPortConfig(host: String, port: Int): Config = {
    val configMap = new JHashMap[String, Any]()
    configMap.put("akka.remote.netty.hostname", host)
    configMap.put("akka.remote.netty.port", port)
    // this is just so that the intellij shuts up
    ConfigFactory.parseMap(configMap.asInstanceOf[JHashMap[String, _ <: AnyRef]])
  }

  private def createActorSystemConfig(host: String, port: Int): Config = {
    val hostPortConfig = createHostPortConfig(host, port)
    val baseAkkaConfig = ConfigFactory.load("akka.conf")
    hostPortConfig.withFallback(baseAkkaConfig)
  }

  private def createAdmin(host: String, port: Int, supervisorRef: ActorRef) = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer

    val info = new SiriusInfo(port, host, supervisorRef)
    new SiriusAdmin(info, mbeanServer)
  }

}