package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import akka.actor.{ActorRef, actorRef2Scala, Actor}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import akka.util.duration._

object MembershipActor {

  /**
   * Create a MembershipActor configured with SiriusConfiguration that will keep membershipAgent
   * updated.
   *
   * @param membershipAgent the Agent[Set[ActorRef]]() to keep updated with the cluster membership
   * @param confing SiriusConfiguration object to use to configure this instance- see SiriusConfiguraiton
   *          for more information
   */
  def apply(membershipAgent: Agent[Set[ActorRef]],
            config: SiriusConfiguration): MembershipActor = {
    val clusterConfigPath = config.getProp[String](SiriusConfiguration.CLUSTER_CONFIG) match {
      case Some(path) => Path.fromString(path)
      case None => throw new IllegalArgumentException(SiriusConfiguration.CLUSTER_CONFIG + " is not configured")
    }
    val checkIntervalSecs = config.getProp(SiriusConfiguration.MEMBERSHIP_CHECK_INTERVAL, 30)

    new MembershipActor(
      membershipAgent,
      clusterConfigPath,
      checkIntervalSecs seconds
    )
  }
}

/**
 * Actor responsible for keeping membership information up to date.
 *
 * For production code you should use MembershipActor#apply instead, this will take care
 * of more proper construction and DI.
 *
 * @param membershipAgent An Agent[Set[ActorRef]] that this actor will keep populated
 *          with the most up to date membership information
 * @param clusterConfigPath A scalax.file.Path containing the membership information
 *          for this cluster
 * @param checkInterval how often to check for updates to clusterConfigPath
 */
class MembershipActor(membershipAgent: Agent[Set[ActorRef]],
                      clusterConfigPath: Path,
                      checkInterval: Duration = (30 seconds)) extends Actor {

  val logger = Logging(context.system, "Sirius")

  val configCheckSchedule = context.system.scheduler.schedule(checkInterval, checkInterval, self, CheckClusterConfig)

  override def preStart() {
    logger.info("Initializing MembershipActor to check {} every {}", clusterConfigPath, checkInterval)

    updateMembership()
  }

  override def postStop() {
    configCheckSchedule.cancel()
  }


  def receive = {
    case CheckClusterConfig =>
      updateMembership()

    case GetMembershipData => sender ! membershipAgent()
  }

  private def updateMembership() {
    val newMembership = createMembership(clusterConfigPath)
    membershipAgent send newMembership

    if (membershipAgent.get != newMembership) {
      logger.info("Updated membership. New value: {}", newMembership)
    }
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
