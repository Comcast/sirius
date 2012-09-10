package com.comcast.xfinity.sirius.util

import akka.actor._

object AkkaExternalAddressResolver extends ExtensionKey[AkkaExternalAddressResolver]

/**
 * Class for figuring out a local actor refs remote address.  This is weird, and I got
 * the gist of it from the following guys:
 *
 *  https://groups.google.com/forum/?fromgroups=#!searchin/akka-user/full$20address/akka-user/POngjU9UpK8/wE74aYiWdWIJ
 *  http://doc.akka.io/docs/akka/snapshot/scala/serialization.html
 *
 * Don't use directly, akka does some voodoo to make the ExtendedActorSystem available.
 *
 * Suggested usage is as follows:
 *
 *    AkkaExternalAddressResolver(actorSystem).externalAddressFor(actorRef)
 *
 */
class AkkaExternalAddressResolver(system: ExtendedActorSystem) extends Extension {

  // the idea here is to discover our external address, by getting our address
  //  relative to an external host, "akka://@nohost:0"
  private final val externalAddress =
    system.provider.getExternalAddressFor(new Address("akka", "", "nohost", 0))

  /**
   * Return the String representation of the external path of the passed in ActorRef
   * if such exists, otherwise return the internal representation (no host/port or anything).
   *
   * This allegedly just works for Netty transport, but this is all we use.
   *
   * @param ref an ActorRef which must be local to the system on which this instance
   *          was created
   * @return the as externally of a representation of this actor as possible-
   *          full path with host and port if the local ActorSystem is set up with
   *          remoting, else the path locally.  Either way, this should be globally
   *          unique to all other paths this ActorRef can get to.
   */
  def externalAddressFor(ref: ActorRef): String = externalAddress match {
    case None => ref.path.toString
    case Some(addr) => ref.path.toStringWithAddress(addr)
  }
}