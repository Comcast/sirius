package com.comcast.xfinity.sirius.tool.format

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

object OrderedEventFormatter {

  private val defaultFormatter = "com.comcast.xfinity.sirius.tool.format.DefaultFormatter"

  val formatter: OrderedEventFormatter = {
    val formatterClass = System.getProperty("eventFormatter", defaultFormatter)
    val clazz = Class.forName(formatterClass)
    clazz.newInstance().asInstanceOf[OrderedEventFormatter]
  }

  def printEvent(evt: OrderedEvent) {
    println(formatter.format(evt))
  }
}

/**
 * Trait/interface for pretty formatting ordered events
 */
trait OrderedEventFormatter {
  def format(evt: OrderedEvent): String
}