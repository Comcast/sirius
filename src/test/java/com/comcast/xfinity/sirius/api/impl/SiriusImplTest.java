package com.comcast.xfinity.sirius.api.impl;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.comcast.xfinity.sirius.api.RequestHandler;

@RunWith(MockitoJUnitRunner.class)
public class SiriusImplTest {
    
    @Mock
    private RequestOrderer requestOrderer;
    
    @Mock
    private RequestHandler requestHandler;
    
    @Mock
    private ExecutorService executorService;
    
    @Mock
    private HttpServletRequest httpServletRequest;
    
    private SiriusImpl sirius;
    
    @Before
    public void setUp() {
        sirius = new SiriusImpl();
        ReflectionTestUtils.setField(sirius, "requestOrderer", requestOrderer);
        ReflectionTestUtils.setField(sirius, "executorService", executorService);
    }
    
    @Test
    public void testEnqueueUpdateCallsOrderRequest() {
        when(httpServletRequest.getMethod()).thenReturn("PUT");
        sirius.enqueueUpdate(httpServletRequest, requestHandler);
        
        verify(requestOrderer).orderRequest(httpServletRequest);
    }
    
    @Test
    public void testThatEnqueueUpdatePassesCommandToExecutorService() {
        when(httpServletRequest.getMethod()).thenReturn("PUT");
        sirius.enqueueUpdate(httpServletRequest, requestHandler);
        ArgumentCaptor<UpdateRunnable> runnableCaptor = ArgumentCaptor.forClass(UpdateRunnable.class);
        
        verify(executorService).execute(runnableCaptor.capture());
        SiriusCommand capturedCommand = runnableCaptor.getValue().getCommand();
        assertEquals(requestHandler, capturedCommand.getHandler());
        assertEquals(httpServletRequest, capturedCommand.getRequest());
    }
    
    @Test
    public void testThatEnqueueGetPassesCommandToExecutorService() {
        when(httpServletRequest.getMethod()).thenReturn("PUT");
        sirius.enqueueGet(httpServletRequest, requestHandler);
        ArgumentCaptor<GetCallable> callableCaptor = ArgumentCaptor.forClass(GetCallable.class);
        
        verify(executorService).submit(callableCaptor.capture());
        SiriusCommand capturedCommand = callableCaptor.getValue().getCommand();
        assertEquals(requestHandler, capturedCommand.getHandler());
        assertEquals(httpServletRequest, capturedCommand.getRequest());
    }
}
