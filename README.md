<p align="center"><img src="logo.png" width="300px"/></p>

- - - -

CodeFeedr
--------------------------

[![Build Status](https://travis-ci.org/codefeedr/codefeedr.svg?branch=develop)](https://travis-ci.org/codefeedr/codefeedr)
[![BCH compliance](https://bettercodehub.com/edge/badge/codefeedr/codefeedr?branch=develop)](https://bettercodehub.com/)
[![Coverage Status](https://coveralls.io/repos/github/codefeedr/codefeedr/badge.svg?branch=develop)](https://coveralls.io/github/codefeedr/codefeedr?branch=develop)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![University](https://img.shields.io/badge/university-tudelft-blue.svg)](http://swerl.tudelft.nl/bin/view/Main/WebHome)

A framework for easily building Flink streaming programs. Documentation can be found [here](https://codefeedr.readthedocs.io).
## Contributors
Main contributors:
- [Wouter Zorgdrager](https://www.linkedin.com/in/wouter-zorgdrager-a4746512a/)

Former contributors:
- [Jos Kuijpers](https://nl.linkedin.com/in/jos-kuijpers-4b714032)
- [Joris Quist](https://www.linkedin.com/in/joris-quist-a44245170) 


## Configuring the build environment

### Command line

The project is build with SBT. To install SBT, do:

* Mac: `brew install sbt`
* Debian/Ubuntu: `apt-get install sbt`

Run `sbt`. Then at the SBT console:

- `compile` to compile
- `run` to run the default class
- `test` to run the tests
- `clean` to clean

### From IntelliJ

Install the latest IntelliJ. Go to the IntelliJ preferences and install the
Scala plugin. Then

1. File -> New -> Project with existing sources from within IntelliJ or "Import project" from the
IntelliJ splash screen
2. Select the top level checkout directory for CodeFeedr.
3. On the dialog that appears, select "SBT"
4. Click Next and then Finish
5. From the "SBT Projet Data To Import", select all modules

In order to run your application from within IntelliJ, you have to select the classpath of the
'mainRunner' module in  the run/debug configurations. Simply open 'Run -> Edit configurations...'
and then select 'mainRunner' from the "Use  classpath of module" dropbox.

## Installing dependencies

CodeFeedr depends on the following external programs:

* [Kafka](https://kafka.apache.org) is used as intermediate storage between
nodes in the CodeFeedr processing graph.
* [Zookeeper](https://zookeeper.apache.org) is used to share
information between CodeFeedr jobs (e.g. output schemata).
It is also a dependency to Kafka.

The following components are optional and their use depends on which / how
plug-ins have been configured:

* [MongoDB](https://www.mongodb.com) is used to cache things that may be
useful for future use, e.g. API responses that will never change
* [Redis](https://redis.io/) For high performance key management

At the moment, CodeFeedr uses default configurations for connecting to
all external services.

The dependencies can be simply installed by using `docker-compose up` with the
provided `docker/docker-compose.yml` file as input. If you are working on a Mac,
use the `docker/docker-compose-mac.yml` file.
