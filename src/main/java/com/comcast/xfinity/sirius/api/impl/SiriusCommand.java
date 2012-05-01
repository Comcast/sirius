package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;

import com.comcast.xfinity.sirius.api.RequestHandler;


final public class SiriusCommand {
    
    final private HttpServletRequest request;
    final private RequestHandler handler;
    final private long order;
    final private SiriusCommandType type;
    
    public SiriusCommand(HttpServletRequest request, RequestHandler handler) {
        this(request, handler, 0);
    }
    
    public SiriusCommand(HttpServletRequest request, RequestHandler handler, long order) {
        this.request = request;
        this.type = SiriusCommandType.getType(request);
        this.handler = handler;
        this.order = order;
    }
    
    public HttpServletRequest getRequest() {
        return request;
    }
    
    public RequestHandler getHandler() {
        return handler;
    }
    
    public long getOrder() {
        return order;
    }

    public SiriusCommandType getType() {
        return type;
    }
}
