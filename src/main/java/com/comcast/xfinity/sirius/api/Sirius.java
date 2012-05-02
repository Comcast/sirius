package com.comcast.xfinity.sirius.api;

import java.util.concurrent.Future;


/**
 * Main interface for the Sirius library.
 */
public abstract class Sirius {
    private RequestHandler requestHandler;
    
    public Sirius(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }
    
    protected RequestHandler getRequestHandler(){
        return requestHandler;
    }
    
    public abstract Future<SiriusResponse> enqeuePUT(String key, Object body);
    
    public abstract Future<SiriusResponse> enqueueDELETE(String key);
    
    public abstract Future<SiriusResponse> enqueueGET(String key);
    
}
