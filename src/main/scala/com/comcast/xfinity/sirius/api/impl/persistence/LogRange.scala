package com.comcast.xfinity.sirius.api.impl.persistence

sealed trait LogRange

case class BoundedLogRange(start: Long, end: Long) extends LogRange
case object EntireLog extends LogRange
