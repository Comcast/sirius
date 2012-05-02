package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.Sirius;
import com.comcast.xfinity.sirius.api.SiriusResponse;


public class SiriusImpl extends Sirius {
    
    public SiriusImpl(RequestHandler requestHandler) {
        super(requestHandler);
    }

    @Inject
    private ExecutorService executorService;
    
    @Override
    public Future<SiriusResponse> enqeuePUT(String key, Object body) {
        RequestCallable callable = new RequestCallable("PUT", key, body, getRequestHandler());
        return executorService.submit(callable);
    }

    @Override
    public Future<SiriusResponse> enqueueDELETE(String key) {
        RequestCallable callable = new RequestCallable("DELETE", key, null, getRequestHandler());
        return executorService.submit(callable);
    }

    @Override
    public Future<SiriusResponse> enqueueGET(String key) {
        RequestCallable callable = new RequestCallable("GET", key, null, getRequestHandler());
        return executorService.submit(callable);
    }
     
}