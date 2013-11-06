package com.comcast.xfinity.sirius.tool.format

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

class DefaultFormatter extends OrderedEventFormatter {
  def format(evt: OrderedEvent): String = evt.toString()
}