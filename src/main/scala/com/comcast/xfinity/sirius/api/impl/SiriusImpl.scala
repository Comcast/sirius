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
import membership.MembershipData

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem, val walWriter: LogWriter, val nodeToJoin: Option[ActorRef]) extends Sirius with AkkaConfig {

  def this(requestHandler: RequestHandler, actorSystem: ActorSystem) = this (requestHandler, actorSystem, new FileLogWriter("/tmp/sirius_wal.log", new WriteAheadLogSerDe()), None)

  def this(requestHandler: RequestHandler, actorSystem: ActorSystem, walWriter: LogWriter) = this (requestHandler, actorSystem, walWriter, None)

  val mbeanServer = ManagementFactory.getPlatformMBeanServer()
  val info = new SiriusInfo(DEFAULT_PORT, InetAddress.getLocalHost().getHostName()) // TODO: Pass in the hostname and port (perhaps)
  val admin = new SiriusAdmin(info, mbeanServer)

  // TODO: we may want to identify these actors by their class name? make debugging direct
  var supervisor = actorSystem.actorOf(Props(new SiriusSupervisor(admin, requestHandler, walWriter)), "sirius")
  // XXX: automatically join the cluster, are we sure this is right?
  supervisor ! JoinCluster(nodeToJoin, info)

  
  def getMembershipMap = {
    (supervisor ? GetMembershipData()).asInstanceOf[Future[Map[SiriusInfo, MembershipData]]]
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
}