package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;


public interface RequestOrderer {
    
    public long orderRequest(HttpServletRequest request);
    
}
