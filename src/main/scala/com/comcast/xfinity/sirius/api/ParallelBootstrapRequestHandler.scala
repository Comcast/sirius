package com.comcast.xfinity.sirius.api

import java.util.concurrent.ConcurrentHashMap

class ParallelBootstrapRequestHandler(val requestHandler: RequestHandler) extends RequestHandler {
    private var sequences: ConcurrentHashMap[String, Long] = _

    override def handleGet(key: String): SiriusResult = requestHandler.handleGet(key)
    override def handlePut(key: String, body: Array[Byte]): SiriusResult = requestHandler.handlePut(key, body)
    override def handleDelete(key: String): SiriusResult = requestHandler.handleDelete(key)

    override def onBootstrapStarting(): Unit = {
        sequences = new ConcurrentHashMap[String, Long]()
        requestHandler.onBootstrapStarting()
    }

    override def onBootstrapComplete(): Unit = {
        // allow the GC to collect the map
        sequences = null
        requestHandler.onBootstrapComplete()
    }

    override def handlePut(sequence: Long, key: String, body: Array[Byte]): SiriusResult = {
        latest(key, sequence) {
            requestHandler.handlePut(sequence, key, body)
        }
    }

    override def handleDelete(sequence: Long, key: String): SiriusResult = {
        latest(key, sequence) {
            requestHandler.handleDelete(sequence, key)
        }
    }

    private def latest(key: String, sequence: Long)(f: => SiriusResult) = {
        val sequences = this.sequences
        if (sequences != null) {
            var result = SiriusResult.none()
            sequences.compute(key, (_, existing) => {
                if (existing < sequence) {
                    result = f
                    sequence
                } else existing
            })
            result
        } else {
            f
        }
    }
}
