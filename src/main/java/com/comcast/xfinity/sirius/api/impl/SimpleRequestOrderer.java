package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;


public class SimpleRequestOrderer implements RequestOrderer {
    
    private long counter = 0;
    
    @Override
    public long orderRequest(HttpServletRequest request) {
        return ++counter;
    }
    
}
