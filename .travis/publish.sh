#!/bin/bash -x

tagVer=${TRAVIS_TAG}
tagVer=${tagVer#v}   # Remove `v` at beginning.
tagVer=${tagVer%%#*} # Remove anything after `#`.
publishVersion='set every version := "'$tagVer'"'

echo "$PGP_PASSPHRASE" | gpg --passphrase-fd 0 --batch --yes --import .travis/secret-key.asc

sbt "$publishVersion" "++$TRAVIS_SCALA_VERSION" "; publishSigned; sonatypeBundleRelease"