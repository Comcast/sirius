package com.comcast.xfinity.sirius.api.impl;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.RequestMethod;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SiriusImplTest {

    @Mock
    private RequestHandler requestHandler;

    @Mock
    private ExecutorService executorService;


    private SiriusImpl sirius;

    @Before
    public void setUp() {

        sirius = new SiriusImpl();

        ReflectionTestUtils.setField(sirius, "executorService", executorService);
        ReflectionTestUtils.setField(sirius, "requestHandler", requestHandler);
    }

    @Test
    public void whenEnqueueIsCalled_RequestIsEnqueued() throws InterruptedException, ExecutionException {
        Future<byte[]> expectedFuture = new MockFuture<byte[]>("testFuture".getBytes());
        when(executorService.submit(any(RequestCallable.class))).thenReturn(expectedFuture);
        when(requestHandler.handle(RequestMethod.GET, "foo", "bar".getBytes())).thenReturn("baz".getBytes());
        Future<byte[]> future = sirius.enqueue(RequestMethod.GET, "foo", "bar".getBytes());
        assertEquals("testFuture", new String(future.get()));
    }

}
