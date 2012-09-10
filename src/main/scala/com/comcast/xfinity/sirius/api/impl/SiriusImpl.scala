package com.comcast.xfinity.sirius.api.impl

import compat.AkkaFutureAdapter
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
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import java.util.{HashMap => JHashMap}

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances
 */
object SiriusImpl extends AkkaConfig {

  /**
   * @see [[com.comcast.xfinity.sirius.api.impl.SiriusFactory$]]
   */
  @deprecated("Moved to SiriusFactory", "2012-08-10")
  def createSirius(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration): SiriusImpl =
    SiriusFactory.createInstance(requestHandler, siriusConfig)

  /**
   * USE ONLY FOR TESTING TO MOCK OUT A LOG!
   * @see [[com.comcast.xfinity.sirius.api.impl.SiriusFactory$]]
   */
  @deprecated("Moved to SiriusFactory", "2012-08-10")
  private[sirius] def createSirius(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration,
                   siriusLog: SiriusLog): SiriusImpl = {
    SiriusFactory.createInstance(requestHandler, siriusConfig, siriusLog)
  }

  /**
   * @see [[com.comcast.xfinity.sirius.api.impl.SiriusFactory$]]
   */
  @deprecated("Moved to SiriusFactory", "2012-08-10")
  private[sirius] def createSirius(requestHandler: RequestHandler, siriusLog: SiriusLog, hostName: String, port: Int,
                   clusterConfigPath: String, usePaxos: Boolean): SiriusImpl = {
    SiriusFactory.createInstance(requestHandler, siriusLog, hostName, port, clusterConfigPath, usePaxos)
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

  val supName = config.getProp(SiriusConfiguration.SIRIUS_SUPERVISOR_NAME, SiriusImpl.DEFAULT_SUPERVISOR_NAME)
  val usePaxos = config.getProp(SiriusConfiguration.USE_PAXOS, false)

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
