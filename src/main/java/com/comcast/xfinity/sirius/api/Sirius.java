package com.comcast.xfinity.sirius.api;

import java.util.concurrent.Future;

/**
 * Main interface for the Sirius library.
 */
public interface Sirius {

    public <BODY, RESPONSE> Future<RESPONSE> enqueue(String method, String key,
            BODY body, RequestHandler<BODY, RESPONSE> requestHandler);

}
