package com.comcast.xfinity.sirius.api.zookeeper

import com.comcast.xfinity.sirius.writeaheadlog.{LogData, LogDataSerDe}
import com.comcast.xfinity.sirius.api.{RequestHandler, SiriusResult}

class SiriusZookeeperImpl(q: DistributedQueue, serDe: LogDataSerDe, handler: RequestHandler )  {

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    handler.handleGet(key)
  }

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    LogData("PUT", key,"",System.currentTimeMillis(),Some(body))
    val cmd = serDe.serialize().getBytes("utf-8")
    q.offer(cmd)
    handle

  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    LogData("DELETE", key,"",System.currentTimeMillis(),None)
    val cmd = serDe.serialize().getBytes("utf-8")
    q.offer(cmd)
    new SiriusResult(cmd)

  }

}