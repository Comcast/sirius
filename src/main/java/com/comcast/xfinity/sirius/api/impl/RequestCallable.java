package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Callable;

import com.comcast.xfinity.sirius.api.RequestHandler;

public class RequestCallable implements Callable {

    private String method;
    private String key;
    private Object body;
    private RequestHandler requestHandler;
    
    public RequestCallable(String method, String key, Object body, RequestHandler requestHandler){
        this.method = method;
        this.key = key;
        this.body = body;
        this.requestHandler = requestHandler;
    }
    
    @Override
    public Object call() throws Exception {
        return requestHandler.handleRequest(key, method, body);
    }
    
}
