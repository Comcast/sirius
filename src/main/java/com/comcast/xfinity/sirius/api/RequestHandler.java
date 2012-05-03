package com.comcast.xfinity.sirius.api;

/**
 * Class that the application must implement to process GETs, PUTs, and DELETEs
 * from the Sirius Queue.
 * 
 */
public interface RequestHandler {

    public byte[] handle(RequestMethod method, String key, byte[] body);

}
