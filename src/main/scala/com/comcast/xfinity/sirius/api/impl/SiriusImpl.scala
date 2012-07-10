package com.comcast.xfinity.sirius.api.impl

import java.lang.management.ManagementFactory
import java.net.InetAddress
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.writeaheadlog.SiriusFileLog
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import akka.pattern.ask
import akka.dispatch.Future
import membership._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import com.typesafe.config.ConfigFactory
import akka.actor._

/**Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances. */
object SiriusImpl extends AkkaConfig {

  def createSirius(requestHandler: RequestHandler, siriusLog: SiriusLog, hostname: String, port: Int): SiriusImpl = {


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
           hostname = """ + hostname + """
           port = """ + port + """
         }
       }

    }""")

    val config = ConfigFactory.load("akka.conf")

    val allConfig = remoteConfig.withFallback(config)

    new SiriusImpl(requestHandler, ActorSystem(SYSTEM_NAME, allConfig), siriusLog, port)
  }
}

/**
 * A Sirius implementation implemented in Scala using Akka actors.
 */
class SiriusImpl(requestHandler: RequestHandler, val actorSystem: ActorSystem, siriusLog: SiriusLog, port: Int)
  extends Sirius with AkkaConfig {

  //TODO: find better way of building SiriusImpl ...
  def this(requestHandler: RequestHandler, actorSystem: ActorSystem) =
    this (requestHandler, actorSystem,
      new SiriusFileLog("/var/lib/sirius/xfinityapi/wal.log", // TODO: Abstract this to the app using Sirius.
        new WriteAheadLogSerDe()), SiriusImpl.DEFAULT_PORT)

  def this(requestHandler: RequestHandler, actorSystem: ActorSystem, walWriter: SiriusLog) =
    this (requestHandler, actorSystem, walWriter, SiriusImpl.DEFAULT_PORT)

  // TODO: Pass in the hostname and port (perhaps)
  val info = new SiriusInfo(port, InetAddress.getLocalHost.getHostName)
  val membershipAgent: Agent[MembershipMap] = Agent(MembershipMap())(actorSystem)
  val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState())(actorSystem)

  val supervisor = createSiriusSupervisor(actorSystem, requestHandler, info, siriusLog, siriusStateAgent,
    membershipAgent)


  def joinCluster(nodeToJoin: Option[ActorRef]) {
    supervisor ! JoinCluster(nodeToJoin, info)
  }


  def getMembershipMap = {
    (supervisor ? GetMembershipData).asInstanceOf[Future[MembershipMap]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    (supervisor ? Get(key)).asInstanceOf[Future[SiriusResult]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    (supervisor ? Put(key, body)).asInstanceOf[Future[SiriusResult]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    (supervisor ? Delete(key)).asInstanceOf[Future[SiriusResult]]
  }

  def shutdown() {
    //TODO: Leave cluster
    membershipAgent.close()
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  // XXX: handle for testing
  private[impl] def createSiriusSupervisor(theActorSystem: ActorSystem, theRequestHandler: RequestHandler,
                                           siriusInfo: SiriusInfo, theWalWriter: SiriusLog,
                                           theSiriusStateAgent: Agent[SiriusState],
                                           theMembershipAgent: Agent[MembershipMap]) = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val admin = new SiriusAdmin(info, mbeanServer)
    val supProps = Props(
      new SiriusSupervisor(admin, theRequestHandler, theWalWriter, theSiriusStateAgent, theMembershipAgent, info))
    theActorSystem.actorOf(supProps, "sirius")
  }
}
