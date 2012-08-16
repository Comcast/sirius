package com.comcast.xfinity.sirius.info

import akka.agent.Agent


import com.comcast.xfinity.sirius.api.impl.{membership, AkkaConfig}
import membership._
import akka.dispatch.Await
import org.apache.commons.lang.builder.ReflectionToStringBuilder
import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import java.util.concurrent.TimeoutException

/**
 * An MBean that exposes information on this Sirius node.
 *
 */
class SiriusInfo(val port: Int, val hostName: String,
                supervisorRef:ActorRef)
  extends SiriusInfoMBean with AkkaConfig {


  /**
   * Gets the name of this Sirius node.
   */
  def getName: String = "sirius-" + hostName + ":" + port

  override def toString: String = getName

  override def getMembership: String = {


    try {
      val membership = Await.result((supervisorRef ? GetMembershipData), timeout.duration)
      ReflectionToStringBuilder.toString(membership)
    } catch {
      case toe: TimeoutException =>
        "Timeout"
      case e: Exception =>  "Unknown"
    }
  }

  override def getLatestSlot: String = {
    try{
    Await.result((supervisorRef ? GetLowestUnusedSlotNum), timeout.duration).asInstanceOf[LowestUnusedSlotNum].slot.toString
    }catch {
       case toe: TimeoutException =>
        "Timeout"
      case e: Exception =>  "Unknown"
    }

  }
}