package com.comcast.xfinity.sirius.api.impl;

import com.comcast.xfinity.sirius.api.RequestHandler;

public class CapturingRequestHandler implements RequestHandler {

    public String key;
    public String method;
    public Object body;
    
    @Override
    public Object handleRequest(String key, String method, Object body) {
        this.key = key;
        this.method = method;
        this.body = body;
        return null;
    }

}
