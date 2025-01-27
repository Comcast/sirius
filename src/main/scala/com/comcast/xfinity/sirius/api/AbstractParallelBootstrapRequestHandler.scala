package com.comcast.xfinity.sirius.api

import java.util.concurrent.ConcurrentHashMap

abstract class AbstractParallelBootstrapRequestHandler[K, M] extends RequestHandler {
    private var sequences: Option[ConcurrentHashMap[K, Long]] = None

    final override def onBootstrapStarting(): Unit = {
        onBootstrapStartingImpl()
        sequences = Some(new ConcurrentHashMap[K, Long]())
    }

    final override def onBootstrapComplete(): Unit = {
        sequences = None
        onBootstrapCompletedImpl()
    }

    final override def handleGet(key: String): SiriusResult =
        if (enabled()) handleGetImpl(createKey(key))
        else SiriusResult.none()

    final override def handlePut(key: String, body: Array[Byte]): SiriusResult =
        if (enabled()) handlePutImpl(createKey(key), deserialize(body))
        else SiriusResult.none()

    final override def handleDelete(key: String): SiriusResult =
        if (enabled()) handleDeleteImpl(createKey(key))
        else SiriusResult.none()

    final override def handlePut(sequence: Long, key: String, body: Array[Byte]): SiriusResult =
        if (enabled())
            sequences match {
                case Some(map) =>
                    val k = createKey(key)
                    // Check if a newer sequence is already in the map and, if so, bail early
                    if (map.get(k) >= sequence) SiriusResult.none()
                    else {
                        var result: SiriusResult = SiriusResult.none()
                        // deserialize the body before calling compute to reduce lock contention
                        val message = deserialize(body)
                        map.compute(k, (_, existing) => {
                            if (existing < sequence) {
                                result = handlePutImpl(sequence, k, message)
                                sequence
                            } else existing
                        })
                        result
                    }
                case None => handlePutImpl(sequence, createKey(key), deserialize(body))
            }
        else SiriusResult.none()

    final override def handleDelete(sequence: Long, key: String): SiriusResult =
        sequences match {
            case Some(map) =>
                var result: SiriusResult = SiriusResult.none()
                map.compute(createKey(key), (k, existing) => {
                    if (existing < sequence) {
                        result = handleDeleteImpl(sequence, k)
                        sequence
                    } else existing
                })
                result
            case None => handleDeleteImpl(sequence, createKey(key))
        }

    protected def enabled(): Boolean
    protected def createKey(key: String): K
    protected def deserialize(body: Array[Byte]): M

    def onBootstrapStartingImpl(): Unit = { }
    def onBootstrapCompletedImpl(): Unit = { }
    def handleGetImpl(key: K): SiriusResult
    def handlePutImpl(key: K, body: M): SiriusResult
    def handleDeleteImpl(key: K): SiriusResult
    def handlePutImpl(sequence: Long, key: K, body: M): SiriusResult = handlePutImpl(key, body)
    def handleDeleteImpl(sequence:Long, key: K): SiriusResult = handleDeleteImpl(key)
}
