package com.comcast.xfinity.sirius.api.impl

import java.lang.management.ManagementFactory
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import com.comcast.xfinity.sirius.writeaheadlog.FileLogWriter
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.pattern.ask
import akka.actor.Props

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem, val walWriter: LogWriter) extends Sirius with AkkaConfig {

  def this(requestHandler: RequestHandler, actorSystem: ActorSystem) = this (requestHandler, actorSystem, new FileLogWriter("/tmp/sirius_wal.log", new WriteAheadLogSerDe()))

  val mbeanServer = ManagementFactory.getPlatformMBeanServer()
  val admin = new SiriusAdmin(2552, mbeanServer)

  // TODO: we may want to identify these actors by their class name? make debugging direct
  var supervisor = actorSystem.actorOf(Props(new SiriusSupervisor(admin, requestHandler, walWriter)), "sirius")


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