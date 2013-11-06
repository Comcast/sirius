package com.comcast.xfinity.sirius.util

import akka.event.slf4j.Slf4jEventHandler
import akka.event.Logging._

/**
 * Akka Logging event handler for suppressing unnecessarily noisy
 * Remoting errors.
 *
 * Suppresses all Error and Warn level messages that start with
 * "REMOTE: RemoteClient" or "REMOTE: RemoteServer".  Turns out this
 * is a pretty good qualifier.
 *
 * If you want to see remoting errors, register an event handler for
 * them to be handled explicitly.
 *
 * The need for this rises from akka.remote.RemoteTransport line 194,
 * where these errors are logged.
 *
 * Credit to
 * https://groups.google.com/forum/?fromgroups=#!topic/akka-user/hLGkCjnGQZc
 * for pointing out that this had to be done this way
 *
 */
class Slf4jEventHandlerWithRemotingSilencer extends Slf4jEventHandler {

  override def receive = {
    case Error(_, _, _, msg) if messageIsStupid(msg) => // no-op
    case Warning(_, _, msg) if messageIsStupid(msg) => // no-op
    case other =>
      passThru(other)
  }

  private def messageIsStupid(msg: Any): Boolean = msg match {
    case msgString: String =>
      msgString.startsWith("REMOTE: RemoteClient") || msgString.startsWith("REMOTE: RemoteServer")
    case _ => false
  }

  private[util] def passThru(event: Any) {
    super.receive(event)
  }
}