package com.comcast.xfinity.sirius.api
import scala.reflect.BeanProperty

/**
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
class SiriusConfiguration {
  @BeanProperty var host: String = _
  @BeanProperty var port: Int = _
  @BeanProperty var clusterConfigPath: String = _
  @BeanProperty var usePaxos: Boolean = true
  @BeanProperty var logLocation: String = _
}