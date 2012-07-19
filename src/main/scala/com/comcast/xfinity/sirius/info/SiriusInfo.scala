package com.comcast.xfinity.sirius.info

/**
 * An MBean that exposes information on this Sirius node.
 * 
 */
class SiriusInfo(val port: Int, val hostName: String) extends SiriusInfoMBean {

  /**
   * Gets the name of this Sirius node.
   */
  def getName: String = "sirius-" + hostName + ":" + port

  override def toString : String = getName

}