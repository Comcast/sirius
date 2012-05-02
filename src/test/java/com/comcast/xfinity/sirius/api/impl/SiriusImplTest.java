package com.comcast.xfinity.sirius.api.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.comcast.xfinity.sirius.api.RequestHandler;

@RunWith(MockitoJUnitRunner.class)
public class SiriusImplTest {
    
    @Mock
    private RequestHandler requestHandler;
    
    @Mock
    private ExecutorService executorService;
    
    private CapturingRequestHandler capturingRequestHandler;
    
    private SiriusImpl sirius;
    
    @Before
    public void setUp() {
        capturingRequestHandler = new CapturingRequestHandler();
        sirius = new SiriusImpl(capturingRequestHandler);
        ReflectionTestUtils.setField(sirius, "executorService", executorService);
    }
    
    @Test
    public void whenEnqueuePUTIsCalled_PutIsEnqueued(){
        sirius.enqeuePUT("foo", "bar");
        
        assertEquals("foo", capturingRequestHandler.key);
        assertEquals("bar", capturingRequestHandler.body);
        assertEquals("PUT", capturingRequestHandler.method);
    }
    
    @Test
    public void whenEnqueueGETIsCalled_GetIsEnqueued(){
        sirius.enqueueGET("baz");

        assertEquals("baz", capturingRequestHandler.key);
        assertEquals("GET", capturingRequestHandler.method);
    }
    
    @Test
    public void whenEnqueueDELETEIsCalled_DELETEIsEnqueued(){
        sirius.enqueueDELETE("quz");

        assertEquals("quz", capturingRequestHandler.key);
        assertEquals("DELETE", capturingRequestHandler.method);
    }
    
}
