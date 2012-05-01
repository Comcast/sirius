package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.Sirius;


public class SiriusImpl implements Sirius {
    
    @Inject
    private RequestOrderer requestOrderer;
    
    @Inject
    private ExecutorService executorService;
    
    @Override
    public void enqueueUpdate(HttpServletRequest request, RequestHandler handler) {
        long order = getOrder(request);
        SiriusCommand command = new SiriusCommand(request, handler, order);
        writeToTransactionLog(command);
        executorService.execute(new UpdateRunnable(command));
    }

    @Override
    public Future<HttpServletResponse> enqueueGet(HttpServletRequest request, RequestHandler handler) { 
        SiriusCommand command = new SiriusCommand(request, handler);
        return executorService.submit(new GetCallable(command));
    }

    private void writeToTransactionLog(SiriusCommand command) {
        
    }

    private long getOrder(HttpServletRequest request) {
        return requestOrderer.orderRequest(request);
    }     
}