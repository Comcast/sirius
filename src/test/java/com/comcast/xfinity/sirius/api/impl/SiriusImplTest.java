package com.comcast.xfinity.sirius.api.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.RequestMethod;

@RunWith(MockitoJUnitRunner.class)
public class SiriusImplTest {

    @Mock
    private RequestHandler requestHandler;

    private ExecutorService executorService;


    private SiriusImpl sirius;

    @Before
    public void setUp() {
        
        sirius = new SiriusImpl();
        
        executorService = new MockExecutorService();
        
        ReflectionTestUtils.setField(sirius, "executorService", executorService);
        ReflectionTestUtils.setField(sirius, "requestHandler", requestHandler);
    }

    @Test
    public void whenEnqueueIsCalled_RequestIsEnqueued() throws InterruptedException, ExecutionException {
        when(requestHandler.handle(RequestMethod.GET, "foo", "bar".getBytes())).thenReturn("baz".getBytes());
        Future<byte[]> future = sirius.enqueue(RequestMethod.GET, "foo", "bar".getBytes());
        assertEquals("baz", new String(future.get()));
    }

}
