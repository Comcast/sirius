package com.comcast.xfinity.sirius.api.impl

import compat.AkkaFutureAdapter
import java.lang.management.ManagementFactory
import java.net.InetAddress
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.pattern.ask
import akka.dispatch.{Future => AkkaFuture}
import membership._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import com.typesafe.config.ConfigFactory
import akka.actor._
import java.util.concurrent.Future
import scalax.file.Path
import com.comcast.xfinity.sirius.writeaheadlog.{WriteAheadLogSerDe, SiriusFileLog, SiriusLog}

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances
 */
object SiriusImpl extends AkkaConfig {

  /**
   * SiriusImpl factory method, takes parameters to construct a SiriusImplementation and the dependent
   * ActorSystem and return the created instance.
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
  def createSirius(requestHandler: RequestHandler,  siriusLog: SiriusLog, hostName: String, port: Int,
                   clusterConfigPath: String, usePaxos: Boolean) = {

    val remoteConfig = ConfigFactory.parseString("""
    akka {
      remote {
         # If this is "on", Akka will log all outbound messages at DEBUG level, if off then
         #they are not logged
         log-sent-messages = off

         # If this is "on", Akka will log all inbound messages at DEBUG level, if off then they are not logged
         log-received-messages = off


         transport = "akka.remote.netty.NettyRemoteTransport"
         netty {
           # if this is set to actual hostname, remote messaging fails... can use empty or the IP address.
           hostname = """" + hostName + """"
           port = """ + port + """
         }
       }

    }""")
    val config = ConfigFactory.load("akka.conf")
    val allConfig = remoteConfig.withFallback(config)
    implicit val actorSystem = ActorSystem(SYSTEM_NAME, allConfig)

    new SiriusImpl(requestHandler, siriusLog, Path.fromString(clusterConfigPath),
      hostName, port, usePaxos)
  }

  @deprecated("Please use 6 arg createSirius", "7-30-12")
  def createSirius(requestHandler: RequestHandler,
                   siriusLog: SiriusLog,
                   hostname: String,
                   port: Int,
                   clusterConfigPath: String): SiriusImpl = {
    createSirius(requestHandler, siriusLog, hostname, port, clusterConfigPath, false)
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
                 host: String = InetAddress.getLocalHost.getHostName,
                 port: Int = 2552,
                 usePaxos: Boolean = false,
                 supName: String = "sirius")
                (implicit val actorSystem: ActorSystem) extends Sirius with AkkaConfig {

  val membershipAgent = Agent(Set[ActorRef]())(actorSystem)
  val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState())(actorSystem)

  val supervisor = createSiriusSupervisor(actorSystem, requestHandler, host, port, siriusLog, siriusStateAgent,
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
    val siriusStateSnapshot = siriusStateAgent()
    (
      (siriusStateSnapshot.stateActorState == SiriusState.StateActorState.Initialized) &&
      (siriusStateSnapshot.membershipActorState == SiriusState.MembershipActorState.Initialized) &&
      (siriusStateSnapshot.persistenceState == SiriusState.PersistenceState.Initialized)
    )

  }

  def checkClusterConfig = {
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

  def shutdown() {
    shutdown(false)
  }

  def shutdown(killActorSystem: Boolean) {
    supervisor ! Kill
    membershipAgent.close()
    if (killActorSystem) {
      actorSystem.shutdown()
      actorSystem.awaitTermination()
    }
  }

  // XXX: handle for testing, now that it's getting crowded we should consider alternative patterns
  private[impl] def createSiriusSupervisor(theActorSystem: ActorSystem, theRequestHandler: RequestHandler, host: String,
                                           port: Int, theWalWriter: SiriusLog, theSiriusStateAgent: Agent[SiriusState],
                                           theMembershipAgent: Agent[Set[ActorRef]], clusterConfigPath: Path,
                                           theSupName: String) = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val info = new SiriusInfo(port, host)
    val admin = new SiriusAdmin(info, mbeanServer)
    val siriusId = host + ":" + port
    val supProps = Props(new
        SiriusSupervisor(admin, theRequestHandler, theWalWriter, theSiriusStateAgent, theMembershipAgent, siriusId,
          clusterConfigPath, usePaxos))
    theActorSystem.actorOf(supProps, theSupName)
  }
}
