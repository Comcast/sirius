package com.comcast.xfinity.sirius.api;

/**
 * Class that the application must implement to process GETs, PUTs, and DELETEs
 * from the Sirius Queue.
 * 
 */
public interface RequestHandler {
    
    public Object handleRequest(String key, String method, Object body);
    
}
