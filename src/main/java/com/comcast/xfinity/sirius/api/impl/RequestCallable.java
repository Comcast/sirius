package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Callable;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.RequestMethod;

public class RequestCallable implements Callable<byte[]> {

    private RequestMethod method;
    private String key;
    private byte[] body;
    private RequestHandler requestHandler;
    
    public RequestCallable(RequestMethod method, String key, byte[] body, RequestHandler requestHandler){
        this.method = method;
        this.key = key;
        this.body = body;
        this.requestHandler = requestHandler;
    }

    public byte[] call() throws Exception {
        return requestHandler.handle(method, key, body);
    }
    
}
