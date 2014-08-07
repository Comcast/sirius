/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl

import java.io.File
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.util.{HashMap => JHashMap}

import com.comcast.xfinity.sirius.admin.ObjectNameHelper
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.{SiriusFactory => NewSiriusFactory}
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.writeaheadlog.CachedSiriusLog
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.typesafe.config.{Config, ConfigFactory}

import akka.actor.ActorRef
import akka.actor.ActorSystem
import javax.management.ObjectName
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

/**
 * Provides the factory for [[com.comcast.xfinity.sirius.api.impl.SiriusImpl]] instances. Please note that
 * this is no longer the recommended way of creating a Sirius implementation; this factory ought to have
 * lived in the top-level API package, and really ought to have returned a trait rather than a concrete.
 * This implementation is preserved for backwards compatibility, although it delegates to a corrected
 * [[com.comcast.xfinity.sirius.api.SiriusFactory]].
 */
@deprecated(message = "Please use com.comcast.xfinity.sirius.api.SiriusFactory instead",
  since = "1.3.0")
object SiriusFactory {
  /**
   * SiriusImpl factory method, takes parameters to construct a SiriusImplementation and the dependent
   * ActorSystem and return the created instance.  Calling shutdown on the produced SiriusImpl will also
   * shutdown the dependent ActorSystem.
   *
   * @param requestHandler the RequestHandler containing callbacks for manipulating the system's state
   * @param siriusConfig a SiriusConfiguration containing configuration info needed for this node.
   * @see SiriusConfiguration for info on needed config.
   *
   * @return A SiriusImpl constructed using the parameters
   */
  def createInstance(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration): SiriusImpl = {
    NewSiriusFactory.createInstance(requestHandler, siriusConfig).asInstanceOf[SiriusImpl]
  }

  /**
   * USE ONLY FOR TESTING HOOK WHEN YOU NEED TO MOCK OUT A LOG.
   * Real code should use the two argument factory method.
   *
   * @param requestHandler the RequestHandler containing callbacks for manipulating the system's state
   * @param siriusConfig a SiriusConfiguration containing configuration info needed for this node.
   * @see SiriusConfiguration for info on needed config.
   * @param siriusLog the persistence layer to which events should be committed to and replayed from.
   *
   * @return A SiriusImpl constructed using the parameters
   */
  private[sirius] def createInstance(requestHandler: RequestHandler, siriusConfig: SiriusConfiguration,
                   siriusLog: SiriusLog): SiriusImpl = {
    NewSiriusFactory.createInstance(requestHandler, siriusConfig, siriusLog).asInstanceOf[SiriusImpl]
  }

}
