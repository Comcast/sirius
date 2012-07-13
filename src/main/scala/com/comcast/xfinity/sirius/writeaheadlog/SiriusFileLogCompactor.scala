package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.{Delete, Put, OrderedEvent}


class SiriusFileLogCompactor(log : SiriusFileLog) {

  def compactLog() : List[OrderedEvent] = {
    val mostRecentPerKey = log.foldLeft(Map[String, OrderedEvent]()) {
      case (acc, event @ OrderedEvent(_, _, Put(key, _))) => acc + (key -> event)
      case (acc, event @ OrderedEvent(_, _, Delete(key))) => acc + (key -> event)
    }
    mostRecentPerKey.values.toList.sortWith(_.sequence < _.sequence)
  }

}