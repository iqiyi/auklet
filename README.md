Table of Contents
=================

   * [Introduction](#introduction)
      * [Features](#features)
      * [Limitation](#limitation)
      * [Why Auklet](#why-auklet)
      * [Why build Auklet upon Swift and Hummingbird](#why-build-auklet-upon-swift-and-hummingbird)
   * [License](#license)
   * [Getting Started](#getting-started)
      * [Prepare a native Swift environment](#prepare-a-native-swift-environment)
      * [Patch the existing Swift](#patch-the-existing-swift)
      * [Deploy Auklet](#deploy-auklet)
      * [Have fun](#have-fun)
   * [Contact](#contact)
   * [Contributing](#contributing)
   * [Acknowledgments](#acknowledgments)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)

# Introduction
[![Slack](https://auklet-slackin.herokuapp.com/badge.svg)](https://auklet-slackin.herokuapp.com/badge.svg)

Auklet is an reimplementation of OpenSwift Swift object server and aims at solving the problem when serving lots of small files in OpenStack Swift.

It is based on 2 existing projects

* OpenStack [Swift](https://github.com/openstack/swift)
* [Hummingbird](https://github.com/troubling/hummingbird)

It does not change any API of OpenStack Swift which means there is no need to change any code of existing clients.

## Features

* Keep most of OpenStack Swift feature list
* Address LOSF problem efficiently
* Optimization for large object storage. Behaviour like replication engine when object is large.
* Lazy migration for replication engine(experimental). ZERO migration effort from existing Swift replication engine!

## Limitation

* No EC support

## Why Auklet
So why implement a new object server when there are Swift and Hummingbird?

Well, in iQIYI, we have tons of images and text files to save into Swift every day and some use cases are very sensitive to request latency. As we know, Swift supports 2 storage engines, namely erasure coding and replication. Unfortunely, neither of them is adequate to serve lots of small files(LOSF).

Hummingbird tries to reimplement OpenStack Swift in Golang but it does not address the LOSF problem yet.

Some other open source projects such as[LinkedIn Ambry](https://github.com/linkedin/ambry) are good choices for small files. But at iQIYI, we have already run so many businesses upon Swift that there would be a big bit gap for us to switch to a new storage platform.

## Why build Auklet upon Swift and Hummingbird
Why not just build Auklet upon single one platform like Swift or Hummingbird?

The essential idea to address LOSF problem is packing small objects into a single POSIX file which has been proved in Facebook's [Haystack](https://code.facebook.com/posts/685565858139515/needle-in-a-haystack-efficient-storage-of-billions-of-photos/) and LinkedIn [Ambry](https://github.com/linkedin/ambry).

As we know, Swift uses multiple concurrent WSGI servers to serve requests which makes it is not suitable to implement the idea mentioned above because extra complex concurrent coordination is required.

About Hummingbird, it is not mature yet so we cannot use it to replace Swift completely in our infrastructure at the moment. But it has at least implemented a nearly full feature prototype.

Then  we decided to implement a new storage engine to address LOSF problem by leveraging the code base of Hummingbird. And initially, the new storage engine will work together with Swift to provide a full feature object storage platform. In fact, the new storage engine is just another implementation of replication engine but in order to make the transfer smoothly, we decides to give it a new name, pack engine.

Finally, we decided to release the project as open source so that any one encountes the same issue could benefit from it. Up to now, it has already run for more than 1 year in our production environment and it does solve the problem as expected.

# License

This project is licensed under Apache License 2.0. See the [LICENSE](LICENSE) file.

This project may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

# Getting Started
Since Auklet only implement part of the features of OpenStack Swift, an existing Swift environment is required.

## Prepare a native Swift environment
NOTE: Only kilo version of OpenStack Swift is well tested at the moment!

Auklet may not work properly in [SAIO](https://docs.openstack.org/swift/latest/development_saio.html), so we recommend installing a minimal Swift environment following our [Tiny Swift guide](doc/tiny-swift/README.md) which is more lightweight than SAIO.

## Patch the existing Swift
There is a workaround to run Auklet with OpenStack Swift without touching the code of Swift. But it is dangerous since it could cause data deleted by Swift object replicator if you are not careful enough. Thus we provide a [patch](patches/pack-policy.patch) to get the job simple.

NOTE: Here we assume that you install Swift environment following our tiny Swift guide exactly.

```
cp $GOPATH/src/github.com/iqiyi/patches/pack-policy.patch /tmp/
cd /opt/openstack/swift-kilo/lib/python2.7/site-packages/swift
sudo patch -p5 < /tmp/pack-policy.patch
sudo su -c "source /opt/openstack/swift-kilo/bin/activate && swift-init reload all"
```

## Deploy Auklet
* Build Auklet following the [dev guild](doc/develop.md).

* Install binary

```
sudo mv bin/auklet /usr/local/bin
sudo mkdir -p /var/run/auklet
```

* Change policy type by modifying `/etc/swift/swift.conf`.

```
[swift-hash]
swift_hash_path_prefix = changeme
swift_hash_path_suffix = emegnahc

[storage-policy:0]
default = yes
name = gold
policy_type = pack

[swift-constraints]
# Limit maximum object size to 5G
max_file_size = 5368709120
```

* Configure log

```
sudo cp $GOPATH/src/github.com/iqiyi/auklet/etc/zap.yml /etc/swift/
sudo mkdir -p /var/log/auklet
sudo chown swift:swift -R /var/log/auklet
```

## Have fun

* Make sure Swift object server is stopped

```
sudo /opt/openstack/swift-kilo/bin/swift-init stop object
```

* Start Auklet object server

```
sudo /usr/local/bin/auklet start object -l /etc/swift/zap.yml
```

* Create a contaienr

```
curl -v -X PUT http://127.0.0.1:8080/v1/iqiyi/auklet
```

* Upload an object

```
curl -v -X PUT -H "Content-Type: text/plain" -d "Hello World" http://127.0.0.1:8080/v1/iqiyi/auklet/hi
```

# Contact

* Slack: [Slack Channel](https://auklet-slackin.herokuapp.com)
* Email: storage AT dev DOT qiyi DOT com

# Contributing
Any form of contribution is welcomed. Check the [dev doc](doc/develop.md) to see how to setup a dev environment and begin to play with the code.

# Acknowledgments
As mentioned before, Auklet is built upon many existing projects. We want to say thanks for those authors for the help.

* The authors of OpenStack Swift for bring up a simple, scalable object storage platform
* The authors of Hummingbird for guide us to find a shortcut to achieve the goal
* The authors of RocksDB to provide such as high performance KV
* Romain and Alex from OVH for the idea of reclaiming space by file hole punch
* All the authors of projects list under `vendor` dir
