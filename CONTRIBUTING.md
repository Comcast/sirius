Contribution Guidelines
=======================

We love to see contributions to the project and have tried to make it
easy to do so. If you wish to contribute code, then please keep to the
following guidelines to ensure consistency within the codebase and
that we have happy users.  All commits should be submitted via a pull
request from a forked project regardless of the committer's
privileges.

For more details about contributing to github projects see
http://gun.io/blog/how-to-github-fork-branch-and-pull-request/

Documentation
-------------

If you contribute anything that changes the behaviour of the
application, document it in the README or wiki! This includes new
features, additional variants of behaviour and breaking changes.

Make a note of breaking changes in the pull request because they will
need to go into the release notes.

Testing
-------

This project uses [ScalaTest](http://www.scalatest.org/) for its
tests. Although any tests are better than none,
[FunSpec](http://doc.scalatest.org/2.0/index.html#org.scalatest.FunSpec)
tests will be looked on more favourably than other types (such as
FlatSpec or JUnit).  [sirius](/../..) includes
[NiceTest](/../../blob/master/src/test/scala/com/comcast/xfinity/sirius/NiceTest.scala),
a helpful abstract class that extends FunSpec, BeforeAndAfter and
MockitoSugar.

Pull Requests
-------------

* should be from a forked project with an appropriate branch name
* should be narrowly focused with no more than 3 or 4 logical commits
* when possible, address no more than one issue
* should be reviewable in the GitHub code review tool
* should be linked to any issues it relates to (ie issue number after
(#) in commit messages or pull request message)

Expect a long review process for any pull requests that add
functionality or change the behavior of the application.


Commit messages
---------------

Please follow the advice of the
[Phonegap guys](https://github.com/phonegap/phonegap/wiki/Git-Commit-Message-Format)
when crafting commit messages. The advice basically comes down to:

* First line should be maximum 50 characters long
* It should summarise the change and use imperative present tense
* The rest of the commit message should come after a blank line
* We encourage you to use Markdown syntax in the rest of the commit
message
* Preferably keep to an 72 character limit on lines in the rest of the
message.

If a commit is related to a particular issue, put the issue number
after a hash (#) somewhere in the detail. You can put the issue number
in the first line summary, but only if you can also fit in a useful
summary of what was changed in the commit.

Here's an example git message:

```

Allow for a Sirius initialization callback

Adds some messages to the SiriusSupervisor so that you can
send it a message to register interest in an initialization
event and get a message back once that has occurred. Added
an onInitialized() method to SiriusImpl to hook that up to
a callback. This addresses issue #17.
```

Formatting
----------

The rules are simple: use the same formatting as the rest of the code.
The following is a list of the styles we are strict about:

* 2 space indent, no tabs
* a space between if/elseif/catch/etc. keywords and the parenthesis
* 120 character line maximum
