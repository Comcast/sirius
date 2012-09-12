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

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances
 */
object SiriusImpl extends AkkaConfig {

  // Type describing method signature for creating a SiriusSupervisor.  Pretty ugly, but
  // the goal is to slim this down with SiriusConfiguration
  type SiriusSupPropsFactory = (RequestHandler, SiriusLog, SiriusConfiguration,
                                  Agent[SiriusState], Agent[Set[ActorRef]]) => Props

  private val createSiriusSupervisor: SiriusSupPropsFactory =
    (requestHandler: RequestHandler, siriusLog: SiriusLog,
     config: SiriusConfiguration, stateAgent: Agent[SiriusState],
     membershipAgent: Agent[Set[ActorRef]]) => {
       Props(
         SiriusSupervisor(
          requestHandler,
          siriusLog,
          config,
          stateAgent,
          membershipAgent
        )
      )
    }

}

/**
 * A Sirius implementation implemented in Scala built on top of Akka.
 *
 * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
 * @param siriusLog the log to be used for persisting events
 * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration
 *            for more information
 * @param supPropsFactory a factory method for creating the Props of the SiriusSupervisor
 *            *** THIS SHOULD NOT BE USED EXTERNALLY, IT IS ONLY FOR DI, AND WILL PROBABLY CHANGE SOON ***
 * @param actorSystem the actorSystem to use to create the Actors for Sirius (Note, this param will likely be
 *            moved in future refactorings to be an implicit parameter at the end of the argument list)
 */
class SiriusImpl(requestHandler: RequestHandler,
                 siriusLog: SiriusLog,
                 config: SiriusConfiguration = new SiriusConfiguration,
                 supPropsFactory: SiriusImpl.SiriusSupPropsFactory = SiriusImpl.createSiriusSupervisor)
                (implicit val actorSystem: ActorSystem)
    extends Sirius with AkkaConfig {

  @deprecated("clusterConfigPath parameter is deprecated and will not be acknowledged", "2012-09-10")
  def this(requestHandler: RequestHandler,
           siriusLog: SiriusLog,
           clusterConfigPath: Path, // <-- being deprecated
           config: SiriusConfiguration = new SiriusConfiguration,
           supPropsFactory: SiriusImpl.SiriusSupPropsFactory = SiriusImpl.createSiriusSupervisor)
          (implicit actorSystem: ActorSystem) =
    this(requestHandler, siriusLog, config, supPropsFactory)(actorSystem)

  val supName = config.getProp(SiriusConfiguration.SIRIUS_SUPERVISOR_NAME, SiriusImpl.DEFAULT_SUPERVISOR_NAME)

  private[impl] var onShutdownHook: Option[(() => Unit)] = None

  val membershipAgent = Agent(Set[ActorRef]())(actorSystem)
  val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState())(actorSystem)

  val supervisor = actorSystem.actorOf(
    supPropsFactory(
      requestHandler,
      siriusLog,
      config,
      siriusStateAgent,
      membershipAgent
    ),
    supName
  )

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

}
