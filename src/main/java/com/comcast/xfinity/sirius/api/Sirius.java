package com.comcast.xfinity.sirius.api;

import java.util.concurrent.Future;

/**
 * Main interface for the Sirius library.
 */
public interface Sirius {

    public Future<byte[]> enqueue(RequestMethod method, String key, byte[] body);

    public Future<byte[]> enqueuePut(String key, byte[] body);
    
    public Future<byte[]> enqueueGet(String key);
}
