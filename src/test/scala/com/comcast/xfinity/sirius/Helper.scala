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
package com.comcast.xfinity.sirius

import akka.actor.{Actor, Props, ActorRef, ActorSystem}
import org.scalatest.mock.MockitoSugar

object Helper extends MockitoSugar {
  /**
   * Wraps an actor inside another, for the purposes of testing things sent to context.parent.  Messages sent
   * to this actor from outside are forwarded to the "inner" actor, messages sent to the context.parent
   * from inside are forwarded to the "parent" param, usually a probe.
   * @param inner actor to be wrapped, built by Props(new WhateverActor)
   * @param parent probe for catching messages sent to parent
   * @param actorSystem actor system to use for creating actor
   * @return "wrapped" actor
   */
  def wrapActorWithMockedSupervisor(inner: Props, parent: ActorRef, actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(Props(new Actor {
      val innerRef = context.actorOf(inner)
      def receive = {
        case x => if (sender == innerRef) {
          parent forward x
        } else {
          innerRef forward x
        }
      }
    }))
  }
}
