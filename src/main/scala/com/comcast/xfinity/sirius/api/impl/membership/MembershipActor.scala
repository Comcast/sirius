package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import java.util.concurrent.TimeUnit
import akka.actor.{actorRef2Scala, Actor}

/**
 * Actor responsible for orchestrating request related to Sirius Cluster Membership
 */
class MembershipActor(membershipAgent: Agent[MembershipMap], siriusId: String, siriusStateAgent: Agent[SiriusState],
                      clusterConfigPath: Path) extends Actor with AkkaConfig {


  def membershipHelper = new MembershipHelper

  val logger = Logging(context.system, this)

  // TODO: This should not be hard-coded.
  private[membership] lazy val checkInterval: Duration = Duration.create(30, TimeUnit.SECONDS)

  val configCheckSchedule = context.system.scheduler.schedule(Duration.Zero, checkInterval, self, CheckClusterConfig)

  override def preStart() {
    logger.info("Bootstrapping Membership Actor, initializing cluster membership map {}", clusterConfigPath)

    updateMembershipMap()

    siriusStateAgent send ((state: SiriusState) => {
      state.updateMembershipActorState(SiriusState.MembershipActorState.Initialized)
    })
  }

  override def postStop() {
    configCheckSchedule.cancel()
  }


  def receive = {
    // Check the stored lastModifiedTime of the clusterConfigPath file against
    // the actual file on disk, and rebuild membership map if necessary
    case CheckClusterConfig =>

      logger.info("updated membershipMap from config {}", clusterConfigPath)
      updateMembershipMap()
    case GetMembershipData => sender ! membershipAgent()
    case _ => logger.warning("Unrecognized message.")
  }

  private def updateMembershipMap() {
    val newMap = createMembershipMap(clusterConfigPath)
    membershipAgent send newMap
    logger.info("Updated Cluster Config")
  }

  /**
   * Creates a MembershipMap from the contents of the clusterConfigPath file.
   *
   * @param clusterConfigPath a Path containing cluster members, one per line, host:port
   * @return MembershipMap of MembershipData
   */
  private[membership] def createMembershipMap(clusterConfigPath: Path): MembershipMap = {
    val lines = clusterConfigPath.lines(NewLine, includeTerminator = false)
    // XXX should use constants for things like /user/sirius ... for chunks of these akka paths
    // XXX also use those same constants when creating original actors with context.actorOf
    lines.foldLeft(MembershipMap())(
      (membershipMap: MembershipMap, hostAndPort: String) => {
        val supervisorPath = "akka://%s@%s/user/sirius".format(SYSTEM_NAME, hostAndPort)
        val paxosPath = "akka://%s@%s/user/sirius/paxos".format(SYSTEM_NAME, hostAndPort)
        val membershipData = MembershipData(context.actorFor(supervisorPath), context.actorFor(paxosPath))
        membershipMap + (hostAndPort -> membershipData)
      }
    )
  }

}
