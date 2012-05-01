package com.comcast.xfinity;

import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;

/**
 * Main interface for the Sirius library.
 */
public interface Sirius {
    /**
     * Adds a PUT or DELETE to the Queue. Before enqueuing, Sirius will decide
     * an absolute order across the entire cluster of Sirius nodes in the
     * system.
     * 
     * @param request
     */
    void enqueueUpdate(HttpServletRequest request);

    /**
     * Adds a GET to the Queue. Does not consult with other nodes, nor does it
     * give an ordering.
     * 
     * @param request
     * @return A future that will contain the results of the GET.
     */
    Future<?> enqueueGet(HttpServletRequest request);
}
