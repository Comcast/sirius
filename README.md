# Sirius [![Build Status](https://travis-ci.org/Comcast/sirius.png)](https://travis-ci.org/Comcast/sirius)

Sirius is a distributed in memory datastore that uses the Paxos
protocol set to perform it's distributed messaging and updates.


## Structure

The directory structure is rather intuitive, as expected
"src/main/scala" contains source, "src/test/scala" contains tests,
and so on. However, the following, not so standard directories for
CIM projects are worth mentioning:

* src/main/bin- scripts which are dropped into sirius-standalone
* project/- sbt build stuff


## Sirius Standalone

Sirius is a rather complex beast and in turn produces some
development and operational complexity. Sirius Standalone was created
to help make the development and operations process around Sirius
based systems much easier.

The Sirius Standalone tarball is generated from the make-standalone
sbt step.  It packages Sirius along with all of its dependencies and
some helper wrapper scripts into a tarball called sirius-standalone.

### sirius-interactive.sh

For development, sirius-standalone provides "sirius-interactive.sh"
which will spin up a scala shell with Sirius and all of its
dependencies loaded up. It also defines some helper functions for
quickly standing up Sirius instances.  The goal of sirius-standalone
is to provide a simple way to experiment with new features during
development, with the hope of accelerating development.

### waltool

To assist in the management of UberStores, sirius-standalone provides
waltool which helps the end user manage their write ahead logs via
supplying visibility into the logs and log compaction.
