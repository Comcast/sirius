package com.comcast.xfinity.sirius.api;

/**
 * Base class that dispatches to different GET/DELETE/PUT methods. This may be a transitional step to
 * putting those methods on the interface if we prefer.
 *
 */

public abstract class AbstractRequestHandler implements RequestHandler {
    
    @Override
    public byte[] handle(RequestMethod method, String key, byte[] body) {
      
        byte[] result = null;

        if (method == RequestMethod.PUT) {
            result = doPut(key, body);
        } else if (method == RequestMethod.GET) {
            result = doGet(key, body);
        } else if (method == RequestMethod.DELETE) {
            result = doDelete(key, body);
        }

        return result;       
    }

    protected byte[] doDelete(String key, byte[] body) {
        return null;
    }

    protected byte[] doGet(String key, byte[] body) {
        return null;
    }

    protected byte[] doPut(String key, byte[] body) {
        return null;
    }
    
}
