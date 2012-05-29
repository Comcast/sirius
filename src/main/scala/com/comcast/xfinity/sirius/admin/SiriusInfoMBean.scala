package com.comcast.xfinity.sirius.admin

/**
 * Trait for the SiriusMBean.  This is needed because JMX MBeans require both an interface 
 * and implementation class, this compiles down to an interface.
 */
trait SiriusInfoMBean {

  def getName : String
  
}