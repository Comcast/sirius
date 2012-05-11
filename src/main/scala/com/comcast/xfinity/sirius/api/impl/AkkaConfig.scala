package com.comcast.xfinity.sirius.api.impl

import akka.util.Timeout
import akka.util.duration._

trait AkkaConfig {
  implicit val timeout: Timeout = (5 seconds)
}