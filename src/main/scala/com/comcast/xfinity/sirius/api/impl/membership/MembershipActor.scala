package com.comcast.xfinity.sirius.api.impl.membership

import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import akka.actor.{ActorRef, actorRef2Scala, Actor}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import akka.util.duration._
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import scala.collection.immutable.HashMap

object MembershipActor {

  sealed trait MembershipMessage
  case object GetMembershipData extends MembershipMessage
  case object CheckClusterConfig extends MembershipMessage
  case class Ping(sent: Long) extends MembershipMessage
  case class Pong(pingSent: Long) extends MembershipMessage

  private[membership] object PingMembership

  private[membership] trait MembershipInfoMBean {
    def getMembership: String
    def getMembershipRoundTrip: Map[String, Long]
    def getTimeSinceLastPingUpdate: Map[String, Long]
  }

  /**
   * Create a MembershipActor configured with SiriusConfiguration that will keep membershipAgent
   * updated.
   *
   * @param membershipAgent the Agent[Set[ActorRef]]() to keep updated with the cluster membership
   * @param config SiriusConfiguration object to use to configure this instance- see SiriusConfiguraiton
   *          for more information
   */
  def apply(membershipAgent: Agent[Set[ActorRef]],
            config: SiriusConfiguration): MembershipActor = {
    // XXX: we should figure out if we're doing config parsing and injecting it, or doing it within the actor
    //      the advantage of pulling out the config here is it makes testing easier, and it makes it obvious what
    //      config is needed
    val clusterConfigPath = config.getProp[String](SiriusConfiguration.CLUSTER_CONFIG) match {
      case Some(path) => Path.fromString(path)
      case None => throw new IllegalArgumentException(SiriusConfiguration.CLUSTER_CONFIG + " is not configured")
    }
    val checkIntervalSecs = config.getProp(SiriusConfiguration.MEMBERSHIP_CHECK_INTERVAL, 30)
    val pingIntervalSecs = config.getProp(SiriusConfiguration.MEMBERSHIP_PING_INTERVAL, 30)

    new MembershipActor(
      membershipAgent,
      clusterConfigPath,
      checkIntervalSecs seconds,
      pingIntervalSecs seconds
    )(config)
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
 * @param config SiriusConfiguration, used to register monitors
 */
class MembershipActor(membershipAgent: Agent[Set[ActorRef]],
                      clusterConfigPath: Path,
                      checkInterval: Duration,
                      pingInterval: Duration)
                     (implicit config: SiriusConfiguration = new SiriusConfiguration)
    extends Actor with MonitoringHooks{
  import MembershipActor._

  val logger = Logging(context.system, "Sirius")

  val configCheckSchedule = context.system.scheduler.schedule(checkInterval, checkInterval, self, CheckClusterConfig)
  val memberPingSchedule = context.system.scheduler.schedule(pingInterval, pingInterval, self, PingMembership)

  var membershipRoundTripMap = HashMap[String, Long]()
  var lastPingUpdateMap = HashMap[String, Long]()

  override def preStart() {
    logger.info("Initializing MembershipActor to check {} every {}", clusterConfigPath, checkInterval)

    registerMonitor(new MembershipInfo, config)

    updateMembership()
  }

  override def postStop() {
    configCheckSchedule.cancel()
    unregisterMonitors(config)
  }


  def receive = {
    case CheckClusterConfig =>
      updateMembership()

    case GetMembershipData => sender ! membershipAgent()

    case Ping(sent) => sender ! Pong(sent)
    case Pong(pingSent) =>
      val currentTime = System.currentTimeMillis
      val senderPath = sender.path.toString
      membershipRoundTripMap += senderPath -> (currentTime - pingSent)
      lastPingUpdateMap += senderPath -> currentTime

    case PingMembership =>
      val currentTime = System.currentTimeMillis
      membershipAgent.get().foreach(_ ! Ping(currentTime))

  }

  private def updateMembership() {
    val oldMembership = membershipAgent()
    val newMembership = createMembership(clusterConfigPath)
    membershipAgent send newMembership

    if (oldMembership != newMembership) {
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

  class MembershipInfo extends MembershipInfoMBean {
    def getMembership: String = membershipAgent().toString()
    def getMembershipRoundTrip: Map[String, Long] = membershipRoundTripMap
    def getTimeSinceLastPingUpdate: Map[String, Long] = {
      val currentTime = System.currentTimeMillis()
      lastPingUpdateMap.foldLeft(HashMap[String, Long]()){
        case (acc, (key, pingTime)) => acc + (key -> (currentTime - pingTime))
      }
    }
  }
}
