package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;

import com.comcast.xfinity.sirius.api.RequestHandler;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SiriusImplTest {
    
    @Mock
    private RequestOrderer requestOrderer;
    
    @Mock
    private RequestHandler requestHandler;
    
    private SiriusImpl sirius;
    
    @Before
    public void setUp() {
        sirius = new SiriusImpl();
        ReflectionTestUtils.setField(sirius, "requestOrderer", requestOrderer);
    }
    
    @Test
    public void testEnqueueUpdateCallsOrderRequest() {
        HttpServletRequest request = new MockHttpServletRequest();
        sirius.enqueueUpdate(request, requestHandler);
        
        verify(requestOrderer).orderRequest(request);
        
    }
    
    
}
