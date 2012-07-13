package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
import membership.MembershipActor.CheckClusterConfig
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import java.util.concurrent.TimeUnit
import akka.actor.{Cancellable, actorRef2Scala, Actor}

object MembershipActor {
  // message only sent to self
  case object CheckClusterConfig
}

/**
 * Actor responsible for orchestrating request related to Sirius Cluster Membership
 */
class MembershipActor(membershipAgent: Agent[MembershipMap], siriusId: String,
                      siriusStateAgent: Agent[SiriusState],
                      clusterConfigPath: Path) extends Actor with AkkaConfig {


  def membershipHelper = new MembershipHelper

  val logger = Logging(context.system, this)

  private var configLastModified = -1L

  // TODO: This should not be hard-coded.
  private[membership] val checkInterval: Duration = Duration.create(30, TimeUnit.SECONDS)

  var configCheckSchedule: Cancellable = _ // This needs to be set on preStart so checkInterval can be overridden in tests

  override def preStart() {
    logger.info("Bootstrapping Membership Actor, initializing cluster membership map {}",
      clusterConfigPath)

    configCheckSchedule = context.system.scheduler.schedule(Duration.Zero, checkInterval, self, CheckClusterConfig)

    updateMembershipMap()

    siriusStateAgent send ((state: SiriusState) => {
      state.updateMembershipActorState(SiriusState.MembershipActorState.Initialized)
    })
  }

  override def postStop() {
    configCheckSchedule.cancel()
  }

  /**
   * Creates a MembershipMap from the contents of the clusterConfigPath file.
   *
   * @param clusterConfigPath a Path containing cluster members, one per line, host:port
   * @return MembershipMap of MembershipData
   */
  def createMembershipMap(clusterConfigPath: Path): MembershipMap = {
    clusterConfigPath.lines(NewLine,
      includeTerminator = false).foldLeft(MembershipMap())((map: MembershipMap, hostAndPort: String) => {

      // XXX should use constants for things like /user/sirius ... for chunks of these akka paths
      // XXX also use those same constants when creating original actors with context.actorOf
      val akkaString = "akka://" + SYSTEM_NAME + "@" + hostAndPort + "/user/sirius"
      val data: MembershipData =
        MembershipData(context.actorFor(akkaString))
      map + (hostAndPort -> data)
    })
  }

  def receive = {
    // Check the stored lastModifiedTime of the clusterConfigPath file against
    // the actual file on disk, and rebuild membership map if necessary
    case CheckClusterConfig =>
      val currentModifiedTime = clusterConfigPath.lastModified
      if (currentModifiedTime > configLastModified) {
        logger.info("CheckClusterConfig detected {} change, updating MembershipMap", clusterConfigPath)

        updateMembershipMap()
      }
    case GetMembershipData => sender ! membershipAgent()
    case _ => logger.warning("Unrecognized message.")
  }

  private def updateMembershipMap() {
    val newMap = createMembershipMap(clusterConfigPath)
    configLastModified = clusterConfigPath.lastModified
    membershipAgent send newMap
  }

}
