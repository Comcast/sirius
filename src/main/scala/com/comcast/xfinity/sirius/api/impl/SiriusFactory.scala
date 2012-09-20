package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.writeaheadlog.{SiriusLog, CachedSiriusLog}
import java.net.InetAddress
import java.util.{HashMap => JHashMap}
import akka.actor.{ActorRef, ActorSystem}
import management.ManagementFactory
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import javax.management.MBeanServer
import com.typesafe.config.{ConfigFactory, Config}
import java.io.File

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

    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    siriusConfig.setProp(SiriusConfiguration.MBEAN_SERVER, mbeanServer)

    implicit val actorSystem = ActorSystem(SYSTEM_NAME, createActorSystemConfig(host, port, siriusConfig))
    val impl = SiriusImpl(requestHandler, siriusLog, siriusConfig)

    // create some more stuff to expose over mbeans
    val admin = createAdmin(mbeanServer, host, port, impl.supervisor)
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

  /**
   * Creates configuration for the ActorSystem. The config precedence is as follows:
   *   1) host/port config trump all
   *   2) siriusConfig supplied external config next
   *   3) sirius-akka-base.conf, packaged with sirius, loaded with ConfigFactory.load
   */
  private def createActorSystemConfig(host: String, port: Int, siriusConfig: SiriusConfiguration): Config = {
    val hostPortConfig = createHostPortConfig(host, port)
    val externalConfig = createExternalConfig(siriusConfig)
    val baseAkkaConfig = ConfigFactory.load("sirius-akka-base.conf")

    hostPortConfig.withFallback(externalConfig).withFallback(baseAkkaConfig)
  }

  private def createAdmin(mbeanServer: MBeanServer, host: String, port: Int, supervisorRef: ActorRef) = {
    val info = new SiriusInfo(port, host, supervisorRef)
    new SiriusAdmin(info, mbeanServer)
  }


  /**
   * If siriusConfig is such configured, will load up an external configuration
   * for the Akka ActorSystem which is created. The filesystem is checked first,
   * then the classpath, if neither exist, or siriusConfig is not configured as
   * much, then an empty Config object is returned.
   */
  private def createExternalConfig(siriusConfig: SiriusConfiguration): Config =
    siriusConfig.getProp[String](SiriusConfiguration.AKKA_EXTERN_CONFIG) match {
      case None => ConfigFactory.empty()
      case Some(externConfig) =>
        val externConfigFile = new File(externConfig)
        if (externConfigFile.exists()) {
          ConfigFactory.parseFile(externConfigFile)
        } else {
          ConfigFactory.parseResources(externConfig)
        }

    }

}