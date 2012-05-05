package com.comcast.xfinity.sirius.api.impl

import akka.util.Timeout
import akka.util.duration._

trait AkkaConfig {
  val SIRIUS_STATE_WORKER_NAME: String = "worker-sirius_state"
  implicit val timeout: Timeout = (5 seconds)

}