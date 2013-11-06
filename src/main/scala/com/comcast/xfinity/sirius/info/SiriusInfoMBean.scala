package com.comcast.xfinity.sirius.info

/*
 * An MBean exposing the address of this node and whether or not it is terminated.
 *
 * This MBean will remain even if the sirius node crashes.
 */
 trait SiriusInfoMBean {

  /**
   * Get the node's address, as seen externally
   */
  def getNodeAddress: String

}