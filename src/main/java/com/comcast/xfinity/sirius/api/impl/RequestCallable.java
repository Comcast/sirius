package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Callable;

import com.comcast.xfinity.sirius.api.RequestHandler;

public class RequestCallable<BODY, RESPONSE> implements Callable<RESPONSE> {

    private String method;
    private String key;
    private BODY body;
    private RequestHandler<BODY, RESPONSE> requestHandler;
    
    public RequestCallable(String method, String key, BODY body, RequestHandler<BODY, RESPONSE> requestHandler){
        this.method = method;
        this.key = key;
        this.body = body;
        this.requestHandler = requestHandler;
    }

    public RESPONSE call() throws Exception {
        return requestHandler.handle(method, key, body);
    }
    
}
