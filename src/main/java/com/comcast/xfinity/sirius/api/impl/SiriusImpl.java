package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.RequestMethod;
import com.comcast.xfinity.sirius.api.Sirius;

public class SiriusImpl implements Sirius {

    @Inject
    ExecutorService executorService;

    @Inject
    RequestHandler requestHandler;

    public Future<byte[]> enqueue(RequestMethod method, String key, byte[] body) {
        RequestCallable callable = new RequestCallable(method, key, body,
                requestHandler);
        return executorService.submit(callable);
    }
}