package com.comcast.xfinity.sirius.api;

import java.util.concurrent.Future;

/**
 * Main interface for the Sirius library.
 */
public interface Sirius {

    public Future<byte[]> enqueue(String method, String key, byte[] body);

}
