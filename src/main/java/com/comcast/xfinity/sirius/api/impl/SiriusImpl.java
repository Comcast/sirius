package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;

import com.comcast.xfinity.sirius.api.RequestHandler;
import com.comcast.xfinity.sirius.api.Sirius;

public class SiriusImpl implements Sirius {

    @Inject
    ExecutorService executorService;

    public <BODY, RESPONSE> Future<RESPONSE> enqueue(String method, String key,
            BODY body, RequestHandler<BODY, RESPONSE> requestHandler) {
        RequestCallable<BODY, RESPONSE> callable = new RequestCallable<BODY, RESPONSE>(
                method, key, body, requestHandler);
        return executorService.submit(callable);
    }
}