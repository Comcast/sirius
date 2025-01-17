package com.comcast.xfinity.sirius.api

import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}

class ParallelBootstrapRequestHandlerTest extends NiceTest {

    describe("delegates") {
        it("handlePut") {
            val requestHandler = mock[RequestHandler]
            val response = mock[SiriusResult]
            when(requestHandler.handlePut("key1", Array.empty)).thenReturn(response)

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            val result = underTest.handlePut("key1", Array.empty)

            assert(result === response)
            verify(requestHandler).handlePut("key1", Array.empty)
            verifyNoMoreInteractions(requestHandler)
        }
        it("handlePut with sequence") {
            val requestHandler = mock[RequestHandler]
            val response = mock[SiriusResult]
            when(requestHandler.handlePut(1L, "key1", Array.empty)).thenReturn(response)

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            val result = underTest.handlePut(1L, "key1", Array.empty)

            assert(result === response)
            verify(requestHandler).handlePut(1L, "key1", Array.empty)
            verifyNoMoreInteractions(requestHandler)
        }
        it("handleDelete") {
            val requestHandler = mock[RequestHandler]
            val response = mock[SiriusResult]
            when(requestHandler.handleDelete("key1")).thenReturn(response)

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            val result = underTest.handleDelete("key1")

            assert(result === response)
            verify(requestHandler).handleDelete("key1")
            verifyNoMoreInteractions(requestHandler)
        }
        it("handleDelete with sequence") {
            val requestHandler = mock[RequestHandler]
            val response = mock[SiriusResult]
            when(requestHandler.handleDelete(1L, "key1")).thenReturn(response)

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            val result = underTest.handleDelete(1L, "key1")

            assert(result === response)
            verify(requestHandler).handleDelete(1L, "key1")
            verifyNoMoreInteractions(requestHandler)
        }
        it("handleGet") {
            val requestHandler = mock[RequestHandler]
            val response = mock[SiriusResult]
            when(requestHandler.handleGet("key1")).thenReturn(response)

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            val result = underTest.handleGet("key1")

            assert(result === response)
            verify(requestHandler).handleGet("key1")
            verifyNoMoreInteractions(requestHandler)
        }
        it("onBootstrapStarting") {
            val requestHandler = mock[RequestHandler]

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            underTest.onBootstrapStarting()

            verify(requestHandler).onBootstrapStarting()
            verifyNoMoreInteractions(requestHandler)
        }
        it("onBootstrapComplete") {
            val requestHandler = mock[RequestHandler]

            val underTest = new ParallelBootstrapRequestHandler(requestHandler)
            underTest.onBootstrapComplete()

            verify(requestHandler).onBootstrapComplete()
            verifyNoMoreInteractions(requestHandler)
        }
    }

    describe("during bootstrap") {
        it ("drops out-of-order events for the same key") {
            val requestHandler = mock[RequestHandler]
            val underTest = new ParallelBootstrapRequestHandler(requestHandler)

            underTest.onBootstrapStarting()
            verify(requestHandler).onBootstrapStarting()

            underTest.handlePut(1L, "key1", Array.empty)
            underTest.handlePut(4L, "key1", Array.empty)
            underTest.handlePut(3L, "key1", Array.empty)

            verify(requestHandler).handlePut(1L, "key1", Array.empty)
            verify(requestHandler).handlePut(4L, "key1", Array.empty)
            verifyNoMoreInteractions(requestHandler)
        }
        it ("allows out-of-order events for different keys") {
            val requestHandler = mock[RequestHandler]
            val underTest = new ParallelBootstrapRequestHandler(requestHandler)

            underTest.onBootstrapStarting()
            verify(requestHandler).onBootstrapStarting()

            underTest.handlePut(1L, "key1", Array.empty)
            underTest.handlePut(4L, "key1", Array.empty)
            underTest.handlePut(3L, "key2", Array.empty)

            verify(requestHandler).handlePut(1L, "key1", Array.empty)
            verify(requestHandler).handlePut(4L, "key1", Array.empty)
            verify(requestHandler).handlePut(3L, "key2", Array.empty)
            verifyNoMoreInteractions(requestHandler)
        }
    }
}
