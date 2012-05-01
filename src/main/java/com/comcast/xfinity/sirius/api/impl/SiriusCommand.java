package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;

import com.comcast.xfinity.sirius.api.RequestHandler;


public class SiriusCommand {
    
    private HttpServletRequest request;
    private RequestHandler handler;
    private long order;
    
    public SiriusCommand(HttpServletRequest request, RequestHandler handler) {
        this.request = request;
        this.handler = handler;
    }
    
    public HttpServletRequest getRequest() {
        return request;
    }
    
    public void setRequest(HttpServletRequest request) {
        this.request = request;
    }
    
    public RequestHandler getHandler() {
        return handler;
    }
    
    public void setHandler(RequestHandler handler) {
        this.handler = handler;
    }
    
    public long getOrder() {
        return order;
    }
    
    public void setOrder(long order) {
        this.order = order;
    }
}
