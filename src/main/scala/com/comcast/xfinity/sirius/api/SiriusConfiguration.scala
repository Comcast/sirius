package com.comcast.xfinity.sirius.api

object SiriusConfiguration {

  /**
   * Host to bind akka to (string)
   *
   * Takes precedence over all other akka configuration for host
   */
  final val HOST = "sirius.akka.host"

  /**
   * Port to bind akka to (int)
   *
   * Takes precedence over all other akka configuration for port
   */
  final val PORT = "sirius.akka.port"

  /**
   * External akka ActorSystem configuration. It this location exists
   * on the file system it is loaded, else it is loaded from the
   * classpath. (string)
   *
   * @see http://doc.akka.io/docs/akka/2.0.2/general/configuration.html
   * for more information
   */
  final val AKKA_EXTERN_CONFIG = "sirius.akka.system-config-overrides"

  /**
   * Location of cluster membership configuration file (string)
   */
  final val CLUSTER_CONFIG = "sirius.membership.config-path"

  /**
   * How often to check CLUSTER_CONFIG for updates, in seconds (int).
   * Also is used to control how often Supervisor checks for updates to membership.
   */
  final val MEMBERSHIP_CHECK_INTERVAL = "sirius.membership.check-interval-secs"

  /**
   * How often to check the membershipAgent for updates, in seconds (int).
   * Used by the supervisor when determining whether to keep paxos on or off.
   */
  final val PAXOS_MEMBERSHIP_CHECK_INTERVAL = "sirius.supervisor.paxos-check-interval-secs"

  /**
   * Whether or not to use paxos (boolean)
   */
  @deprecated("Paxos is always enabled, this option has no effect", "2012-09-24")
  final val USE_PAXOS = "sirius.paxos.enabled"

  /**
   * Directory to put UberStore in (string)
   */
  final val LOG_LOCATION = "sirius.uberstore.dir"

  /**
   * Name of the sirius supervisor, typically we will not change this,
   * but it's here just in case (string)
   */
  final val SIRIUS_SUPERVISOR_NAME = "sirius.supervisor.name"

  /**
   * Number of milliseconds for a proposal to live with the possibility
   * of being reproposed.  This window is not 100% exact at this point-
   * it's precision is REPROPOSAL_CLEANUP_FREQ (long)
   */
  final val REPROPOSAL_WINDOW = "sirius.paxos.replica.reproposal-window-millis"

  /**
   * How often, in seconds, to reap old proposals (int)
   */
  final val REPROPOSAL_CLEANUP_FREQ = "sirius.paxos.replica.reproposal-freq-secs"

  /**
   * Number of milliseconds for Acceptors to retain PValues. When cleaning up in
   * the Acceptors we remove PValues from the acceptor in slot number order until
   * we encounter one which is within the retention window.  At this point we stop
   * cleaning up. Note that it is possible that PValues outside of the this window
   * may be retained if there is a PValue with a timestamp that is within the window
   * before it. (long)
   *
   * Similar to REPROPOSAL_WINDOW, precision is ACCEPTOR_CLEANUP_FREQ
   */
  final val ACCEPTOR_WINDOW = "sirius.paxos.acceptor.acceptor-window-millis"

  /**
   * How often, in seconds, to clean up the Acceptor (int)
   */
  final val ACCEPTOR_CLEANUP_FREQ = "sirius.paxos.acceptor.acceptor-cleanup-freq-secs"

  /**
   * An MBeanServer, that if configured, will be used to expose metrics around Sirius
   * (MBeanServer)
   */
  final val MBEAN_SERVER = "sirius.monitoring.mbean-server"

  /**
   * Number of events to request from a remote node in a single chunk
   */
  final val LOG_REQUEST_CHUNK_SIZE = "sirius.log-request.chunk-size"

  /**
   * How long (in seconds) to wait for a log chunk reply before considering it timed out
   */
  final val LOG_REQUEST_RECEIVE_TIMEOUT_SECS = "sirius.log-request.receive-timeout-secs"

  /**
   * How long (in seconds) to wait between checking for gaps
   */
  final val LOG_REQUEST_FREQ_SECS = "sirius.log-request.freq-secs"

  /**
   * How long (in milliseconds) for requests in SiriusImpl to wait for a response from
   * the underlying actor (int)
   */
  final val CLIENT_TIMEOUT_MS = "sirius.client.ask-timeout-ms"

}

/**
 * Configuration object for Sirius.  Encapsulates arbitrary String key/Any value data.
 *
 * @see SiriusConfiguration$ constants for information of fields.
 */
class SiriusConfiguration {

  import SiriusConfiguration._

  private var conf = Map[String, Any]()

  /**
   * Return the underlying Map[String, Any] configuring this instance
   *
   * Not Java API friendly, if we so find the need, we can add a conversion
   *
   * @return Map[String, Any] of all configuration
   */
  def getConfigMap: Map[String, Any] = conf

  /**
   * Set an arbitrary property on this configuration
   *
   * @param name name of the property
   * @param value value to associate with name
   */
  def setProp(name: String, value: Any) {
    conf += (name -> value)
  }

  /**
   * Get a property from this configuration
   *
   * @param name property name to get
   *
   * @return Some(value) if it exists, or None if not
   */
  def getProp[T](name: String): Option[T] = conf.get(name).asInstanceOf[Option[T]]

  /**
   * Get a property with a default fallback
   *
   * @param name property name to get
   * @param default the value to return if the property doesn't exist
   *
   * @return the value stored under name, or the default if it's not found
   */
  def getProp[T](name: String, default: T): T = conf.get(name) match {
    case Some(value) => value.asInstanceOf[T]
    case None => default
  }

}
