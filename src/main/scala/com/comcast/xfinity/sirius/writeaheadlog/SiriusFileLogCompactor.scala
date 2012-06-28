package com.comcast.xfinity.sirius.writeaheadlog


class SiriusFileLogCompactor(log : SiriusFileLog) {

  def compactLog() : List[LogData] = {
      log.foldLeft[Map[String, LogData]](Map())((acc, logData) => acc + (logData.key -> logData))
  }.values.toList.sortWith(_.sequence < _.sequence)

}