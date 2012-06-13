package com.comcast.xfinity.sirius.api.impl

import java.lang.management.ManagementFactory
import java.net.InetAddress
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.writeaheadlog.FileLogWriter
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Future
import com.typesafe.config.ConfigFactory
import membership._
import akka.agent.Agent

object SiriusImpl extends AkkaConfig {
  
  def createSirius(requestHandler: RequestHandler, walWriter: LogWriter,
          hostname: String, port: Int): SiriusImpl = {
    val config = ConfigFactory.parseString("""
     akka {
       actor {
         provider = "akka.remote.RemoteActorRefProvider"
       }
       remote {
         transport = "akka.remote.netty.NettyRemoteTransport"
         netty {
           hostname = """ + "\"" + hostname + "\"" + """
           port = """ + port.toString + """
         }

       }
     }
     """)

    new SiriusImpl(requestHandler, ActorSystem(SYSTEM_NAME, 
            ConfigFactory.load(config)), walWriter, port)
  }

}

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(requestHandler: RequestHandler,
                 val actorSystem: ActorSystem, 
                 walWriter: LogWriter,
                 port: Int) extends Sirius with AkkaConfig {

  //TODO: find better way of building SiriusImpl ...
  def this(requestHandler: RequestHandler, actorSystem: ActorSystem) = 
    this(requestHandler, actorSystem, new FileLogWriter("/tmp/sirius_wal.log",
        new WriteAheadLogSerDe()), SiriusImpl.DEFAULT_PORT)

  def this(requestHandler: RequestHandler, actorSystem: ActorSystem, walWriter: LogWriter) =
    this (requestHandler, actorSystem, walWriter, SiriusImpl.DEFAULT_PORT)

  // TODO: Pass in the hostname and port (perhaps)
  val info = new SiriusInfo(port, InetAddress.getLocalHost().getHostName()) 
  val membershipAgent: Agent[MembershipMap] = Agent(MembershipMap()) (actorSystem)

  val supervisor = createSiriusSupervisor(actorSystem, requestHandler, info, walWriter, membershipAgent)


  def joinCluster(nodeToJoin: Option[ActorRef]) = {
    supervisor ! JoinCluster(nodeToJoin, info)
  }


  def getMembershipMap() = {
    (supervisor ? GetMembershipData).asInstanceOf[Future[MembershipMap]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    (supervisor ? Get(key)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    (supervisor ? Put(key, body)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    (supervisor ? Delete(key)).asInstanceOf[Future[Array[Byte]]]
  }

  def shutdown() = {
    //TODO: Leave cluster
    membershipAgent.close()
    actorSystem.shutdown()
    actorSystem.awaitTermination()

  }


  // XXX: handle for testing
  private[impl] def createSiriusSupervisor(theActorSystem: ActorSystem, theRequestHandler: RequestHandler,
          siriusInfo: SiriusInfo, theWalWriter: LogWriter, theMembershipAgent: Agent[MembershipMap]) = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer()
    val admin = new SiriusAdmin(info, mbeanServer)
    val supProps = Props(new SiriusSupervisor(admin, theRequestHandler, theWalWriter, theMembershipAgent))
    theActorSystem.actorOf(supProps, "sirius")
  }
}