package com.comcast.xfinity.sirius.admin

import akka.actor.{ActorSystem, ActorRef}
import javax.management.ObjectName
import java.util.{Hashtable => JHashtable}
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver

class ObjectNameHelper {

  /**
   * Create a globally unique object name for an MBean/actor pair
   *
   * @param mbean the MBean to generate an ObjectName for, the resultant ObjectName will be
   *          unique for the MBean per unique actor
   * @param actor actor to generate an Object name for
   * @param actorSystem the actor system this actor was created on, used to derive the name
   *
   * @return an ObjectName globally unique with respect to mbean and actor
   */
  def getObjectName(mbean: Any, actor: ActorRef, actorSystem: ActorSystem): ObjectName = {
    val kvs = new JHashtable[String, String]
    kvs.put("path", "/" + actor.path.elements.reduceLeft(_ + "/" + _))

    val (host, port) = getHostPort(actorSystem)
    kvs.put("host", host)
    kvs.put("port", port)
    kvs.put("sysname", actorSystem.name)

    val statClass = mbean.getClass.getSimpleName
    kvs.put("name", statClass)

    new ObjectName("com.comcast.xfinity.sirius", kvs)
  }

  private def getHostPort(actorSystem: ActorSystem): (String, String) =
    AkkaExternalAddressResolver(actorSystem).externalAddress match {
        case None => ("", "")
        case Some(address) =>
          val host = address.host.getOrElse("")
          val port = address.port match {
            case None => ""
            case Some(portNo) => portNo.toString
          }
          (host, port)
      }
}