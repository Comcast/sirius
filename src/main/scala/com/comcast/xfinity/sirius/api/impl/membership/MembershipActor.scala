package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, actorRef2Scala, Actor}

/**
 * Actor responsible for keeping membership information up to date
 *
 * @param membershipAgent An Agent[Set[ActorRef]] that this actor will keep populated
 *          with the most up to date membership information
 * @param siriusStateAgent An Agent[SiriusState] which this actor will update with
 *          its status on initialization
 * @param clusterConfigPath A scalax.file.Path containing the membership information
 *          for this cluster
 */
class MembershipActor(membershipAgent: Agent[Set[ActorRef]],
                      siriusStateAgent: Agent[SiriusState],
                      clusterConfigPath: Path) extends Actor {

  val logger = Logging(context.system, this)

  def membershipHelper = new MembershipHelper

  // TODO: This should not be hard-coded.
  private[membership] lazy val checkInterval: Duration = Duration.create(30, TimeUnit.SECONDS)

  val configCheckSchedule = context.system.scheduler.schedule(Duration.Zero, checkInterval, self, CheckClusterConfig)

  override def preStart() {
    logger.info("Initializing MembershipActor, initializing cluster membershing using {}", clusterConfigPath)

    updateMembership()

    siriusStateAgent send ((state: SiriusState) => {
      state.updateMembershipActorState(SiriusState.MembershipActorState.Initialized)
    })
  }

  override def postStop() {
    configCheckSchedule.cancel()
  }


  def receive = {
    case CheckClusterConfig =>
      logger.debug("Updating membership from {}", clusterConfigPath)
      updateMembership()

    case GetMembershipData => sender ! membershipAgent()
    case _ => logger.warning("Unrecognized message.")
  }

  private def updateMembership() {
    val newMembership = createMembership(clusterConfigPath)
    membershipAgent send newMembership
  }

  /**
   * Creates a MembershipMap from the contents of the clusterConfigPath file.
   *
   * @param clusterConfigPath a Path containing cluster members, one per line, in akka
   *        actor path format (see http://doc.akka.io/docs/akka/2.0.2/general/addressing.html)
   * @return Set[ActorRef] of all members according to clusterConfigPath
   */
  private[membership] def createMembership(clusterConfigPath: Path): Set[ActorRef] = {
    val lines = clusterConfigPath.lines(NewLine, includeTerminator = false)
    lines.foldLeft(Set[ActorRef]())(
      (membership: Set[ActorRef], actorPath: String) =>
        if (actorPath.charAt(0) == '#')
          membership
        else
          membership + context.actorFor(actorPath)
    )
  }

}
