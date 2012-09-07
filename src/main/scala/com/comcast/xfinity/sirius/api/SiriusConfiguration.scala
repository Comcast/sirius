package com.comcast.xfinity.sirius.api

import scala.reflect.BeanProperty

object SiriusConfiguration {

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
  def create(host: String,
             port: Int,
             clusterConfigPath: String,
             usePaxos: Boolean,
             logLocation: String): SiriusConfiguration = {
    val conf = new SiriusConfiguration()
    conf.host = host;
    conf.port = port;
    conf.clusterConfigPath = clusterConfigPath
    conf.usePaxos = usePaxos
    conf.logLocation = logLocation
    conf
  }

  /**
   * Scala syntax sugar friendly version of create
   * @see create
   */
  def apply(host: String,
            port: Int,
            clusterConfigPath: String,
            usePaxos: Boolean,
            logLocation: String): SiriusConfiguration =
    create(host, port, clusterConfigPath, usePaxos, logLocation)

}

/**
 * Configuration object for Sirius.  Meant to encapsulate some arbitrary,
 * and not so arbitrary configuration data.
 */
// XXX: scaladoc on these bean properties is sort of awkward...
class SiriusConfiguration {
  @BeanProperty var host: String = ""
  @BeanProperty var port: Int = 2552
  @BeanProperty var clusterConfigPath: String = _
  @BeanProperty var usePaxos: Boolean = true
  @BeanProperty var logLocation: String = _

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

}