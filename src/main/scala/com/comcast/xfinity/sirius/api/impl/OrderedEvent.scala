package com.comcast.xfinity.sirius.api.impl

case class OrderedEvent(sequence: Long, timestamp: Long, request: NonCommutativeSiriusRequest)