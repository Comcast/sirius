package com.comcast.xfinity.sirius.api.impl.membership

import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import akka.event.Logging
import akka.util.Duration
import akka.actor.{Props, ActorRef, actorRef2Scala, Actor}
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
   * Create Props for a MembershipActor.
   *
   * @param membershipAgent the Agent[Map[String, Option[ActorRef\]\]\]() to keep updated with the cluster membership
   * @param config SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(membershipAgent: Agent[Map[String, Option[ActorRef]]], config: SiriusConfiguration): Props = {
    // XXX: we should figure out if we're doing config parsing and injecting it, or doing it within the actor
    //      the advantage of pulling out the config here is it makes testing easier, and it makes it obvious what
    //      config is needed
    val clusterConfigPath = config.getProp[String](SiriusConfiguration.CLUSTER_CONFIG) match {
      case Some(path) => Path.fromString(path)
      case None => throw new IllegalArgumentException(SiriusConfiguration.CLUSTER_CONFIG + " is not configured")
    }
    val checkIntervalSecs = config.getProp(SiriusConfiguration.MEMBERSHIP_CHECK_INTERVAL, 30)
    val pingIntervalSecs = config.getProp(SiriusConfiguration.MEMBERSHIP_PING_INTERVAL, 30)
    //Props(classOf[MembershipActor], membershipAgent, clusterConfigPath, checkIntervalSecs seconds, pingIntervalSecs seconds, config)
    Props(new MembershipActor(membershipAgent, clusterConfigPath, checkIntervalSecs seconds, pingIntervalSecs seconds, config))
  }
}

/**
 * Actor responsible for keeping membership information up to date.
 *
 * For production code you should use MembershipActor#apply instead, this will take care
 * of more proper construction and DI.
 *
 * @param membershipAgent An Agent[Map[String, Option[ActorRef\]\]\] that this actor will keep populated
 *          with the most up to date membership information
 * @param clusterConfigPath A scalax.file.Path containing the membership information
 *          for this cluster
 * @param checkInterval how often to check for updates to clusterConfigPath
 * @param config SiriusConfiguration, used to register monitors
 */
class MembershipActor(membershipAgent: Agent[Map[String, Option[ActorRef]]],
                      clusterConfigPath: Path,
                      checkInterval: Duration,
                      pingInterval: Duration,
                      config: SiriusConfiguration)
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

    case GetMembershipData => sender ! membershipAgent.get()

    case Ping(sent) => sender ! Pong(sent)
    case Pong(pingSent) =>
      val currentTime = System.currentTimeMillis
      val senderPath = sender.path.toString
      membershipRoundTripMap += senderPath -> (currentTime - pingSent)
      lastPingUpdateMap += senderPath -> currentTime

    case PingMembership =>
      val currentTime = System.currentTimeMillis
      membershipAgent.get().values.flatten.foreach(_ ! Ping(currentTime))

  }

  /**
   * Creates a MembershipMap from the contents of the clusterConfigPath file.
   */
  private[membership] def updateMembership() {
    val actorPaths = clusterConfigPath.lines(NewLine, includeTerminator = false).toList

    removeMissingPaths(actorPaths)
    updateActorRefs(actorPaths)
  }

  /**
   * If we have any references to actor paths that have been removed from the
   * cluster config, they need to be removed from the agent.
   *
   * @param actorPaths paths for local and remote sirius actors in the cluster
   */
  def removeMissingPaths(actorPaths: List[String]) {
    membershipAgent.get().keys.foreach {
      case key if !actorPaths.contains(key) => membershipAgent send (_ - key)
      case _ =>
    }
  }

  /**
   * For each non-commented line of the ClusterConfig file, try to resolve a remote actor.
   * If the actor is resolved into an actorRef, add it to the membership map. Otherwise, remove
   * any reference to that actor from the map.
   *
   * @param actorPaths list of actor paths to attempt to resolve
   */
  /*
  def updateActorRefs(actorPaths: List[String]) {
    actorPaths
      .filterNot(_.startsWith("#")) // not commented out
      .filter(membership.get(_) == None) // we don't already have a ref for it
      .foreach(path => {
      context.actorSelection(path).resolveOne(1 seconds) onComplete {
        case Success(actor) =>
          membershipAgent send (_ + (path -> Some(actor)))
          // TODO watch the newly created actor, and handle the Terminated message when it comes in
        case Failure(_) =>
          membershipAgent send (_ + (path -> None))
      }
    })
  }
  */
  def updateActorRefs(actorPaths: List[String]) {
    actorPaths
      .filterNot(_.startsWith("#"))
      .foreach(path => {
        membershipAgent send (_ + (path -> Some(context.actorFor(path))))
    })
  }

  class MembershipInfo extends MembershipInfoMBean {
    def getMembership: String = membershipAgent.get().toString()
    def getMembershipRoundTrip: Map[String, Long] = membershipRoundTripMap
    def getTimeSinceLastPingUpdate: Map[String, Long] = {
      val currentTime = System.currentTimeMillis()
      lastPingUpdateMap.foldLeft(HashMap[String, Long]()){
        case (acc, (key, pingTime)) => acc + (key -> (currentTime - pingTime))
      }
    }
  }
}
