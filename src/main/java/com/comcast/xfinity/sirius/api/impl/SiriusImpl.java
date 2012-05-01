package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Callable;
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
        SiriusCommand command = new SiriusCommand(request, handler);
        command.setOrder(order);
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

class UpdateRunnable implements Runnable {

    private SiriusCommand command;
    
    UpdateRunnable(SiriusCommand command){
        this.command = command;
    }
    
    public SiriusCommand getCommand() {
        return command;
    }

    @Override
    public void run() {
        command.getHandler().handleUpdate(command.getRequest());
    }   
}

class GetCallable implements Callable<HttpServletResponse> {

    private SiriusCommand command;
    
    GetCallable(SiriusCommand command){
        this.command = command;
    }

    public SiriusCommand getCommand() {
        return command;
    }

    @Override
    public HttpServletResponse call() throws Exception {
        return command.getHandler().handleGet(command.getRequest());
    }
    
}
