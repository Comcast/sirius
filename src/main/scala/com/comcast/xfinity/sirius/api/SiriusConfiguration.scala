package com.comcast.xfinity.sirius.api

object SiriusConfiguration {

  /**
   * Host to bind akka to (string)
   */
  final val HOST = "sirius.akka.host"

  /**
   * Port to bind akka to (int)
   */
  final val PORT = "sirius.akka.port"

  /**
   * Location of cluster membership configuration file (string)
   */
  final val CLUSTER_CONFIG = "sirius.membership.config-path"

  /**
   * How often to check CLUSTER_CONFIG for updates, in seconds (int)
   */
  final val MEMBERSHIP_CHECK_INTERVAL = "sirius.membership.check-interval-secs"

  /**
   * Whether or not to use paxos (boolean)
   */
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
   * Factory method to create a SiriusConfiguration object.
   *
   * @param hostName the hostName or IP to which this instance should bind.  It is important that other
   *          Sirius instances identify this host by this name.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param port the port which this instance should bind to.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param clusterConfigPath string pointing to the location of this cluster's configuration.  This should
   *          be a file with Akka style addresses on each line indicating membership. For more information
   *          see http://doc.akka.io/docs/akka/snapshot/general/addressing.html
   * @param usePaxos should the underlying implementation use Paxos for ordering events? If true it will,
   *          if not it will use use a simple monotonically increasing counter, which is good enough
   *          as long as this instance isn't clustered
   * @param logLocation location on the filesystem of the Sirius persistent log.
   */
  @deprecated("Don't think this is used, setting fields directly is preferred", "2012-9-10")
  def create(host: String,
             port: Int,
             clusterConfigPath: String,
             usePaxos: Boolean,
             logLocation: String): SiriusConfiguration = {
    val conf = new SiriusConfiguration()
    conf.setProp(HOST, host)
    conf.setProp(PORT, port)
    conf.setProp(CLUSTER_CONFIG, clusterConfigPath)
    conf.setProp(USE_PAXOS, usePaxos)
    conf.setProp(LOG_LOCATION, logLocation)
    conf
  }

  /**
   * Scala syntax sugar friendly version of create
   * @see create
   */
  @deprecated("Don't think this is used, setting fields directly is preferred", "2012-9-10")
  def apply(host: String,
            port: Int,
            clusterConfigPath: String,
            usePaxos: Boolean,
            logLocation: String): SiriusConfiguration =
    create(host, port, clusterConfigPath, usePaxos, logLocation)

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

  /**
   * convenience for setting host to use, going away soon
   */
  @deprecated("use setProp(HOST_KEY) instead", "2012-09-07")
  def setHost(host: String) {
    setProp(HOST, host)
  }

  /**
   * convenience for getting the host to use, going away soon
   */
  def getHost: String = getProp(HOST, "")

  /**
   * convenience for setting the port to use, going away soon
   */
  @deprecated("use setProp(PORT_KEY) instead", "2012-09-07")
  def setPort(port: Int) {
    setProp(PORT, port)
  }

  /**
   * convenience for getting the port to use
   */
  def getPort: Int = getProp(PORT, 2552)

  /**
   * convenience for setting the cluster configuration location, going away soon
   */
  @deprecated("use setProp(CLUSTER_CONFIG_KEY) instead", "2012-09-07")
  def setClusterConfigPath(path: String) {
    setProp(CLUSTER_CONFIG, path)
  }

  /**
   * convenience for getting the cluster configuration location, going away soon
   */
  def getClusterConfigPath: String = getProp(CLUSTER_CONFIG, null)

  /**
   * convenience for setting enabling paxos or not, going away soon
   */
  @deprecated("use setProp(USE_PAXOS_KEY) instead", "2012-09-07")
  def setUsePaxos(usePaxos: Boolean) {
    setProp(USE_PAXOS, usePaxos)
  }

  /**
   * convenience for getting whether or not to use paxos, going away soon
   */
  def getUsePaxos: Boolean = getProp(USE_PAXOS, true)

  /**
   * convenience for setting the location of uberstore, going away soon
   */
  @deprecated("use setProp(LOG_LOCATION_KEY) instead", "2012-09-07")
  def setLogLocation(logDir: String) {
    setProp(LOG_LOCATION, logDir)
  }

  /**
   * convenience for getting the location of uberstore, going away soon
   */
  def getLogLocation: String = getProp(LOG_LOCATION, null)

}
