package com.comcast.xfinity.sirius.api.impl;

import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

class GetCallable implements Callable<HttpServletResponse> {

    private SiriusCommand command;
    
    GetCallable(SiriusCommand command){
        this.command = command;
    }

    public SiriusCommand getCommand() {
        return command;
    }

    @Override
    public HttpServletResponse call() throws Exception {
        return command.getHandler().handleGet(command.getRequest());
    }
    
}