package com.comcast.xfinity.sirius.api.impl;

import javax.servlet.http.HttpServletRequest;

public enum SiriusCommandType {
    GET,
    UPDATE;
    
    public static SiriusCommandType getType(HttpServletRequest request) {
        String method = request.getMethod();
        
        if ("GET".equals(method)) {
            return GET;
        } else if ("PUT".equals(method) || "DELETE".equals(method)) {
            return UPDATE;
        } else {
            throw new IllegalArgumentException("Unsupported HTTP verb: " + method);
        }
    }

}
