package com.comcast.xfinity.sirius.api.zookeeper

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import scala.collection.JavaConversions._
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory
import java.util.List
import java.util.NoSuchElementException
import java.util.TreeMap
import org.apache.jute.compiler.JLong

/**
 * A protocol to implement a distributed queue</a>.
 */
class DistributedQueue(zookeeper: ZooKeeper, dir: String, acl: List[ACL]) {

  private val LOG = LoggerFactory.getLogger(classOf[DistributedQueue])
  val prefix = "qn-"

  def this(zookeeper: ZooKeeper, dir: String) = this (zookeeper, dir, ZooDefs.Ids.OPEN_ACL_UNSAFE)

  /**
   * Returns a Map of the children, ordered by id.
   *
   * @param watcher optional watcher on getChildren() operation.
   * @return map from id to child name for all children
   */
  private def getOrderedChildren(watcher: Watcher): TreeMap[Long, String] = {
    var orderedChildren: TreeMap[Long, String] = new TreeMap[Long, String]
    var childNames: List[String] = null
    try {
      childNames = zookeeper.getChildren(dir, watcher)
    } catch {
      case e: KeeperException.NoNodeException => {
        throw e
      }
    }

    for (childName <- childNames) {
      try {
        //check format
        if (!childName.regionMatches(0, prefix, 0, prefix.length)) {
          LOG.warn("Found child node with improper name: " + childName)
        } else {
          var suffix: String = childName.substring(prefix.length)
          var childId = suffix.toLong
          orderedChildren.put(childId, childName)
        }
      } catch {
        case e: NumberFormatException => {
          LOG.warn("Found child node with improper format : " + childName + " " + e, e)
        }
      }
    }
    return orderedChildren
  }

  /**
   * Find the smallest child node.
   *
   * @return The name of the smallest child node.
   */
  private def smallestChildName: String = {
    var minId = Long.MaxValue
    var minName = ""
    var childNames: List[String] = null
    try {
      childNames = zookeeper.getChildren(dir, false)
    } catch {
      case e: KeeperException.NoNodeException => {
        LOG.warn("Caught: " + e, e)
        return null
      }
    }

    for (childName <- childNames) {
      try {
        if (!childName.regionMatches(0, prefix, 0, prefix.length)) {
          LOG.warn("Found child node with improper name: " + childName)

        } else {
          var suffix: String = childName.substring(prefix.length)
          var childId = suffix.toLong
          if (childId < minId) {
            minId = childId
            minName = childName
          }
        }
      } catch {
        case e: NumberFormatException => {
          LOG.warn("Found child node with improper format : " + childName + " " + e, e)
        }
      }
    }
    if (minId < Long.MaxValue) {
      return minName
    } else {
      return null
    }
  }

  /**
   * Return the head of the queue without modifying the queue.
   *
   * @return the data at the head of the queue.
   * @throws NoSuchElementException
   * @throws KeeperException
   * @throws InterruptedException
   */
  def element: Array[Byte] = {

    var orderedChildren: TreeMap[Long, String] = null
    var toRet: Array[Byte] = null;
    while (toRet == null) {
      try {
        orderedChildren = getOrderedChildren(null)
      } catch {
        case e: KeeperException.NoNodeException => {
          throw new NoSuchElementException
        }
      }
      if (orderedChildren.size == 0) {
        throw new NoSuchElementException
      }

      for (headNode <- orderedChildren.values) {
        if (headNode != null) {
          try {
            toRet = zookeeper.getData(dir + "/" + headNode, false, null)
          } catch {
            case e: KeeperException.NoNodeException => {
              LOG.debug("Head disappeared so try again.")
            }
          }
        }
      }
    }
    toRet
  }

  /**
   * Attempts to remove the head of the queue and return it.
   *
   * @return The former head of the queue
   * @throws NoSuchElementException
   * @throws KeeperException
   * @throws InterruptedException
   */
  def remove: Array[Byte] = {
    var orderedChildren: TreeMap[Long, String] = null
    var data: Array[Byte] = null
    while (data == null) {
      try {
        orderedChildren = getOrderedChildren(null)
      } catch {
        case e: KeeperException.NoNodeException => {
          throw new NoSuchElementException
        }
      }
      if (orderedChildren.size == 0) {
        throw new NoSuchElementException
      }

      for (headNode <- orderedChildren.values) {
        var path: String = dir + "/" + headNode
        try {
          data = zookeeper.getData(path, false, null)
        zookeeper.delete(path, -1)

        } catch {
          case e: KeeperException.NoNodeException => {
            LOG.debug("Attempted to remove {} but was already gone", headNode)
          }
        }
      }
    }
    data
  }

  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   *
   * @return The former head of the queue
   * @throws NoSuchElementException
   * @throws KeeperException
   * @throws InterruptedException
   */
  def take: Array[Byte] = {
    var orderedChildren: TreeMap[Long, String] = null
    var data: Array[Byte] = null
    while (data == null) {
      val childWatcher = new LatchChildWatcher()
      var gotKids = false;
      while (!gotKids) {
        try {
          orderedChildren = getOrderedChildren(childWatcher)
          gotKids = true
        } catch {
          case e: KeeperException.NoNodeException => {
            LOG.debug("Apparently the znode where the queue lives is not there so creating it here: {}", dir)
            zookeeper.create(dir, new Array[Byte](0), acl, CreateMode.PERSISTENT)
          }
        }
      }

      if (orderedChildren.size == 0) {
        childWatcher.await
      } else {
        for (headNode <- orderedChildren.values) {
          val path = dir + "/" + headNode
          try {
            data = zookeeper.getData(path, false, null)
            zookeeper.delete(path, -1)

          } catch {
            case e: KeeperException.NoNodeException => {
              LOG.debug("Attempted to take {}, but it was not there.", path)
            }
          }
        }
      }
    }
    data
  }

  /**
   * Inserts data into queue.
   *
   * @param data
   * @return true if data was successfully added
   */
  def offer(data: Array[Byte]): Boolean = {
    var gotIt = false
    while (!gotIt) {
      try {
        zookeeper.create(dir + "/" + prefix, data, acl, CreateMode.PERSISTENT_SEQUENTIAL)
        gotIt = true
      } catch {
        case e: KeeperException.NoNodeException => {
          zookeeper.create(dir, new Array[Byte](0), acl, CreateMode.PERSISTENT)
        }
      }
    }
    gotIt
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is empty.
   *
   * @return data at the first element of the queue, or null.
   * @throws KeeperException
   * @throws InterruptedException
   */
  def peek: Array[Byte] = {
    try {
      return element
    } catch {
      case e: NoSuchElementException => {
        return null
      }
    }
  }

  /**
   * Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
   *
   * @return Head of the queue or null.
   * @throws KeeperException
   * @throws InterruptedException
   */
  def poll: Array[Byte] = {
    try {
      return remove
    } catch {
      case e: NoSuchElementException => {
        return null
      }
    }
  }

}




