package com.comcast.xfinity.sirius.api.zookeeper

import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.{Watcher, WatchedEvent}
import org.slf4j.LoggerFactory

/**
 * A watcher
 */
class LatchChildWatcher extends Watcher {
  val LOG = LoggerFactory.getLogger(classOf[LatchChildWatcher])
  val latch: CountDownLatch = new CountDownLatch(1)

  def process(event: WatchedEvent): Unit = {
    LOG.debug("Watcher fired on path: " + event.getPath + " state: " + event.getState + " type " + event.getType)
    latch.countDown
  }

  def await: Unit = {
    latch.await
  }
}



