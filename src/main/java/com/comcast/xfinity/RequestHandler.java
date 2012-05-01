package com.comcast.xfinity;

import javax.servlet.http.HttpServletRequest;

/**
 * Class that the application must implement to process GETs, PUTs, and DELETEs
 * from the Sirius Queue.
 * 
 */
public interface RequestHandler {
    /**
     * Processes the PUTs and DELETEs from the Sirius queue and applies business
     * logic.
     * 
     * @param request
     */
    void hanldeUpdate(HttpServletRequest request);

    /**
     * Processes the GETs from the Sirius queue and applies business logic.
     * 
     * @param request
     * @return The response of the GET from the application
     */
    HttpServletRequest handleGet(HttpServletRequest request);
}
