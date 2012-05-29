package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.Future
import akka.pattern.ask
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.writeaheadlog.{WriteAheadLogSerDe, FileLogWriter}

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem) extends Sirius with AkkaConfig {

  var logPath = "/tmp/sirius_wal.log"

  // TODO: we may want to identify these actors by their class name? make debugging direct
  val stateActor = actorSystem.actorOf(Props(new SiriusStateActor(requestHandler)), "state")
  val logWriter = new FileLogWriter(logPath, new WriteAheadLogSerDe())
  val persistenceActor = actorSystem.actorOf(Props(new SiriusPersistenceActor(stateActor,logWriter)), "persistence")
  val paxosActor = actorSystem.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")
  
  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    (stateActor ? Get(key)).asInstanceOf[Future[Array[Byte]]]
  }
  
  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    (paxosActor ? Put(key, body)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    (paxosActor ? Delete(key)).asInstanceOf[Future[Array[Byte]]]
  }
}