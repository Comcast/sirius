package com.comcast.xfinity.sirius.api

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

abstract class AbstractParallelBootstrapRequestHandler[K, M] extends RequestHandler {
    private var sequences: Option[ConcurrentHashMap[K, Long]] = None

    final override def onBootstrapStarting(parallel: Boolean): Unit = {
        onBootstrapStartingImpl(parallel)

        // if in parallel bootstrap mode then create the map used to track sequence by key
        sequences = if (parallel)
            Some(new ConcurrentHashMap[K, Long]())
        else None
    }

    final override def onBootstrapComplete(): Unit = {
        sequences = None
        onBootstrapCompletedImpl()
    }

    final override def handleGet(key: String): SiriusResult =
        handleGetImpl(createKey(key))

    final override def handlePut(key: String, body: Array[Byte]): SiriusResult =
        if (writesEnabled()) handlePutImpl(createKey(key), deserialize(body))
        else SiriusResult.none()

    final override def handleDelete(key: String): SiriusResult =
        if (writesEnabled()) handleDeleteImpl(createKey(key))
        else SiriusResult.none()

    final override def handlePut(sequence: Long, key: String, body: Array[Byte]): SiriusResult =
        if (writesEnabled())
            sequences match {
                case Some(map) =>
                    val k = createKey(key)
                    // Check if a newer sequence is already in the map and, if so, bail early
                    if (map.get(k) >= sequence) SiriusResult.none()
                    else {
                        var result: SiriusResult = SiriusResult.none()
                        // deserialize the body before calling compute to reduce lock contention
                        val message = deserialize(body)

                        // Scala 2.11 has limited support for Java functional interfaces
                        val updateFunction = new BiFunction[K, Long, Long]() {
                            override def apply(key: K, existing: Long): Long = if (existing < sequence) {
                                result = handlePutImpl(sequence, key, message)
                                sequence
                            } else existing
                        }

                        map.compute(k, updateFunction)
                        result
                    }
                case None => handlePutImpl(sequence, createKey(key), deserialize(body))
            }
        else SiriusResult.none()

    final override def handleDelete(sequence: Long, key: String): SiriusResult =
        sequences match {
            case Some(map) =>
                var result: SiriusResult = SiriusResult.none()

                // Scala 2.11 has limited support for Java functional interfaces
                val updateFunction = new BiFunction[K, Long, Long] {
                    override def apply(key: K, existing: Long): Long =
                        if (existing < sequence) {
                            result = handleDeleteImpl(sequence, key)
                            sequence
                        } else existing
                }

                map.compute(createKey(key), updateFunction)
                result
            case None => handleDeleteImpl(sequence, createKey(key))
        }

    protected def writesEnabled(): Boolean = true
    protected def createKey(key: String): K
    @throws[Exception] protected def deserialize(body: Array[Byte]): M

    def onBootstrapStartingImpl(parallel: Boolean): Unit = { }
    def onBootstrapCompletedImpl(): Unit = { }
    def handleGetImpl(key: K): SiriusResult
    def handlePutImpl(key: K, body: M): SiriusResult
    def handleDeleteImpl(key: K): SiriusResult
    def handlePutImpl(sequence: Long, key: K, body: M): SiriusResult = handlePutImpl(key, body)
    def handleDeleteImpl(sequence:Long, key: K): SiriusResult = handleDeleteImpl(key)
}
