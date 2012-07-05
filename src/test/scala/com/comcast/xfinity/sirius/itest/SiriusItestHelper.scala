package com.comcast.xfinity.sirius.itest

import annotation.tailrec
import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, SiriusImpl, SiriusState}

object SiriusItestHelper extends AkkaConfig {
  def waitForInitialization(sirius: SiriusImpl, maxWait: Int = 5000) : Boolean = {
    @tailrec
    def waitForInitializationWorker(start: Long,  maxWait: Int): Boolean = {
      if (System.currentTimeMillis() <= start + maxWait) {
        if (sirius.siriusStateAgent.await(timeout).supervisorState == SiriusState.SupervisorState.Initialized) {
          true
        }
        else waitForInitializationWorker(start, maxWait)
      }
      else {
        false
      }
    }

    waitForInitializationWorker(System.currentTimeMillis(), maxWait)
  }
}