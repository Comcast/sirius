package com.comcast.xfinity.sirius.api

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.util.Timeout
import akka.util.duration._

trait AkkaTestConfig {
  implicit val system = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

  implicit val timeout: Timeout = (1 seconds)

}