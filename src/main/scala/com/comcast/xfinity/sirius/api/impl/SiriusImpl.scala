package com.comcast.xfinity.sirius.api.impl

import compat.AkkaFutureAdapter
import java.lang.management.ManagementFactory
import java.net.InetAddress
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import akka.pattern.ask
import akka.dispatch.{Future => AkkaFuture}
import membership._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.actor._
import java.util.concurrent.Future
import scalax.file.Path
import com.comcast.xfinity.sirius.writeaheadlog.{CachedSiriusLog, SiriusLog}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.uberstore.UberStore
import java.util.{HashMap => JHashMap}
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances
 */
object SiriusImpl extends AkkaConfig {


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
  def createSirius(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration): SiriusImpl = {
    val backendLog = UberStore(siriusConfig.logLocation)
    val log = CachedSiriusLog(backendLog)
    createSirius(requestHandler, siriusConfig, log)
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
  private[sirius] def createSirius(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration,
                   siriusLog: SiriusLog): SiriusImpl = {

    implicit val actorSystem = ActorSystem(SYSTEM_NAME, createActorSystemConfig(siriusConfig))
    val impl = new SiriusImpl(
      requestHandler,
      siriusLog,
      Path.fromString(siriusConfig.getClusterConfigPath),
      siriusConfig
    )

    // create the stuff to expose mbeans
    val admin = createAdmin(siriusConfig: SiriusConfiguration, impl.supervisor)
    admin.registerMbeans()

    // need to shut down the actor system and unregister the mbeans when sirius is done
    impl.onShutdown({
      actorSystem.shutdown()
      actorSystem.awaitTermination()
      admin.unregisterMbeans()
    })

    impl
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
  private[sirius] def createSirius(requestHandler: RequestHandler, siriusLog: SiriusLog, hostName: String, port: Int,
                   clusterConfigPath: String, usePaxos: Boolean): SiriusImpl = {

    val siriusConfig = new SiriusConfiguration
    siriusConfig.host = hostName
    siriusConfig.port = port
    siriusConfig.clusterConfigPath = clusterConfigPath
    siriusConfig.usePaxos = usePaxos

    createSirius(requestHandler, siriusConfig, siriusLog)
  }

  private def createHostPortConfig(siriusConfig: SiriusConfiguration): Config = {
    val configMap = new JHashMap[String, Any]()
    configMap.put("akka.remote.netty.hostname", siriusConfig.host)
    configMap.put("akka.remote.netty.port", siriusConfig.port)
    // this is just so that the intellij shuts up
    ConfigFactory.parseMap(configMap.asInstanceOf[JHashMap[String, _ <: AnyRef]])
  }

  private def createActorSystemConfig(siriusConfig: SiriusConfiguration): Config = {
    val hostPortConfig = createHostPortConfig(siriusConfig)
    val baseAkkaConfig = ConfigFactory.load("akka.conf")
    hostPortConfig.withFallback(baseAkkaConfig)
  }

  private def createAdmin(siriusConfiguration: SiriusConfiguration, supervisorRef: ActorRef) = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer

    val info = new SiriusInfo(siriusConfiguration.port, siriusConfiguration.host, supervisorRef)
    new SiriusAdmin(info, mbeanServer)
  }

}

/**
 * A Sirius implementation implemented in Scala built on top of Akka.
 *
 * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
 * @param siriusLog the log to be used for persisting events
 * @param host the host identifying this node (Developers' note, this is only used for creating an MBean and
 *            siriusId String which is no longer used, we may be able to eliminate it)
 * @param port port this node is bound to (Developers' note, this is only used for creating an MBean and
 *            siriusId String which is no longer used, we may be able to eliminate it)
 * @param clusterConfigPath String pointing to a file containing cluster membership
 * @param usePaxos a flag indicating to use Paxos (if true) or a naiive monotonically incrementing counter
 *            for the ordering of events.  Defaults to false if using Scala.
 * @param supName the name given to the top level sirius supervisor on instantiation
 * @param actorSystem the actorSystem to use to create the Actors for Sirius (Note, this param will likely be
 *            moved in future refactorings to be an implicit parameter at the end of the argument list)
 */
class SiriusImpl(requestHandler: RequestHandler,
                 siriusLog: SiriusLog,
                 clusterConfigPath: Path,
                 config: SiriusConfiguration = new SiriusConfiguration)
                (implicit val actorSystem: ActorSystem)
    extends Sirius with AkkaConfig {

  val supName = config.getProp("sirius.supervisor.name", SiriusImpl.DEFAULT_SUPERVISOR_NAME)
  val usePaxos = config.usePaxos

  private[impl] var onShutdownHook: Option[(() => Unit)] = None

  val membershipAgent = Agent(Set[ActorRef]())(actorSystem)
  val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState())(actorSystem)

  val supervisor = createSiriusSupervisor(actorSystem, requestHandler, siriusLog, siriusStateAgent,
    membershipAgent, clusterConfigPath, supName)

  def getMembership = {
    val akkaFuture = (supervisor ? GetMembershipData).asInstanceOf[AkkaFuture[Set[ActorRef]]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Returns true if the underlying Sirius is up and ready to handle requests
   *
   * @return true if system is ready, false if not
   */
  def isOnline: Boolean = {
    // XXX: the following is not pretty, we will not want to do it this way for real, this is
    //      a sacrifice that must be made to start getting a stable api build though, at the time
    //      this was written, the success rate was 7%
    if (supervisor.isTerminated) {
      false
    } else {
      val siriusStateSnapshot = siriusStateAgent()
      ((siriusStateSnapshot.stateActorState == SiriusState.StateActorState.Initialized) &&
        (siriusStateSnapshot.membershipActorState == SiriusState.MembershipActorState.Initialized) &&
        (siriusStateSnapshot.persistenceState == SiriusState.PersistenceState.Initialized))
    }

  }

  def checkClusterConfig() {
    supervisor ! CheckClusterConfig
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String): Future[SiriusResult] = {
    val akkaFuture = (supervisor ? Get(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]): Future[SiriusResult] = {
    //XXX: this will always return a Sirius.None as soon as Ordering is complete
    val akkaFuture = (supervisor ? Put(key, body)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String): Future[SiriusResult] = {
    val akkaFuture = (supervisor ? Delete(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Set a block of code to run on shutDown
   *
   * @param shutdownHook call by name code to execute
   */
  def onShutdown(shutdownHook: => Unit) {
    onShutdownHook = Some(() => shutdownHook)
  }

  /**
   * Terminate this instance.  Shuts down all associated Actors.
   */
  def shutdown() {
    supervisor ! Kill
    membershipAgent.close()
    siriusStateAgent.close()
    onShutdownHook match {
      case Some(shutdownHook) => shutdownHook()
      case None => //do nothing
    }

  }


  // XXX: handle for testing, now that it's getting crowded we should consider alternative patterns
  private[impl] def createSiriusSupervisor(theActorSystem: ActorSystem,
                                           theRequestHandler: RequestHandler,
                                           theWalWriter: SiriusLog,
                                           theSiriusStateAgent: Agent[SiriusState],
                                           theMembershipAgent: Agent[Set[ActorRef]],
                                           clusterConfigPath: Path,
                                           theSupName: String) = {
    val supProps = Props(
      SiriusSupervisor(theRequestHandler, theWalWriter, theSiriusStateAgent, theMembershipAgent, clusterConfigPath,
        usePaxos))
    theActorSystem.actorOf(supProps, theSupName)
  }

}
