package com.comcast.xfinity.sirius.api.impl

/**
 * Represents an event that must be ordered across the cluster, so probably a PUT or DELETE.
 * 
 * Only requests that do not commute must be ordered.  We're willing to do 'dirty reads' for 
 * commutative requests.
 * 
 * @param sequence Sequence number that totally orders this event across all events and nodes in the cluster.
 * @param timestamp Timestamp which indicates when the event was created.
 * @param request The request to order.
 */
case class OrderedEvent(sequence: Long, timestamp: Long, request: NonCommutativeSiriusRequest)