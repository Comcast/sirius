package com.comcast.xfinity.sirius.itest

import org.junit.rules.TemporaryFolder
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.scalatest.BeforeAndAfterAll
import akka.agent.Agent
import akka.actor.{Props, ActorSystem, ActorRef}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.CheckClusterConfig

@RunWith(classOf[JUnitRunner])
class MembershipITest extends NiceTest with TimedTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("MembershipITest")

  // XXX: this uses some weird voodoo, beware
  val tempFolder = new TemporaryFolder()

  override def afterAll() {
    actorSystem.shutdown()
  }

  before {
    tempFolder.create()
  }

  after {
    tempFolder.delete()
  }

  describe("a SiriusImpl") {
    it("updates its membershipMap after the cluster config file is changed and checkClusterConfig is invoked.") {
      val clusterConfigFileName = tempFolder.newFile("cluster.conf").getAbsolutePath
      val clusterConfigPath = Path.fromString(clusterConfigFileName)
      clusterConfigPath.append(
        "/user/actor1\n" +
        "/user/actor2\n"
      )

      val membership = Agent(Set[ActorRef]())
      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.CLUSTER_CONFIG, clusterConfigFileName)
      val membershipSubSystem = actorSystem.actorOf(Props(MembershipActor(membership, config)))

      // wait for membership to get updated
      assert(waitForTrue(!membership().isEmpty, 2000, 200), "Membership wasn't updated in a timely manor")

      // make sure the first two members exist
      val expected1 = actorSystem.actorFor("/user/actor1")
      val expected2 = actorSystem.actorFor("/user/actor2")
      assert(membership().contains(expected1))
      assert(membership().contains(expected2))

      // add a member to the membership config file
      val actorPath3 = "/user/actor3"
      clusterConfigPath.append(actorPath3 + "\n")
      membershipSubSystem ! CheckClusterConfig

      val expected3 = actorSystem.actorFor(actorPath3)
      assert(waitForTrue(membership().contains(expected3), 2000, 200),
        "Membership map should contain new entry within a certain amount of time"
      )
    }
  }


}
