package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.Sirius;


public class SiriusImpl implements Sirius {
    
    @Inject
    private RequestOrderer requestOrderer;
    
    @Override
    public void enqueueUpdate(HttpServletRequest request, RequestHandler handler) {
        long order = getOrder(request);
        
        SiriusCommand command = new SiriusCommand(request, handler);
        command.setOrder(order);
        
        addToQueue(command);
        writeToTransactionLog(command);
    }

    @Override
    public Future<HttpServletResponse> enqueueGet(HttpServletRequest request, RequestHandler handler) {
        
        //Future<HttpServletResponse> future = new Future<HttpServletResponse>();
        
        SiriusCommand command = new SiriusCommand(request, handler);
        
        addToQueue(command);
        return null;
    }

    private void writeToTransactionLog(SiriusCommand command) {
        // TODO Auto-generated method stub
        
    }

    private void addToQueue(SiriusCommand command) {
        // TODO Auto-generated method stub
        
    }

    private long getOrder(HttpServletRequest request) {
        return requestOrderer.orderRequest(request);
    }     
}
