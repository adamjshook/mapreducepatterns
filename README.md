mapreducepatterns
=================

Repository for MapReduce Design Patterns (O'Reilly 2012) example source code, updated for the Second Edition release.

The code examples now contain MRUnit tests.  We are using a yet-to-be-released snapshot version of MRUnit, 1.2.0-SNAPSHOT, which you will need to build and install yourself... Until 1.2.0 is released and I get around to fixing the version in the pom and then removing this message.  This is necessary to pick up the patch for MRUNIT-221 to fix the generic on the key comparators for a few examples.

Requires git and Apache Maven to build.  Maybe other stuff.

$ git clone http://git-wip-us.apache.org/repos/asf/mrunit.git
$ cd mrunit/
$ mvn clean install

