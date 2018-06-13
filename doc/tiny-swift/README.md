# Introduction
This post introduces how to setup a tiny swift environment which is even more lightweight than [SAIO](https://docs.openstack.org/swift/latest/development_saio.html) so that one can get started with essential part of OpenStack Swift as soon as possible.

### What is a Tiny Swift environment

* Run in Python virtualenv in only one host
* Authentication is disabled
* No multiple hosts emulation like SAIO

Tiny Swift looks like a single node deployment in a Swift cluster except that the authentication is disabled. Even so, it is adequate to demostrate the essential features of Swift such as read/write, replication, audit.


### Supported stack
So far only Swift Kilo version and CentOS 7 is verified. Introductions for other distributions such as Debian, Ubuntu are planed to be added soon. However, since Swift is installed in Python virtualenv, we believe it is no hard to apply the intructions on other stackslike Ubuntu and latest Swift.

# Tiny Swift deployment

### Preequisites
Basically, you need an CentOS 7 box with super user privilege and Internet access.
Run the Swift services as root user is not recommended. And a new user called `swift` will be created.

```
sudo useradd swift
```

Besides, SELinux could be annoying for development. So let's turn it off.

```
sudo setenforce Permissive
```
Note: this setting will be lost on host reboot. If you want to turn it off permanently, refer to Google.


### Installation
* Install dependencies

```
sudo yum group install "Development Tools"  -y
sudo yum install python-virtualenv memcached rsync libffi-devel openssl-devel python-devel -y
```

* Create a virtualenv

```
sudo virtualenv /opt/openstack/swift-kilo
```

* Install Swift Kilo by one line command. It is also possible to install it from source.

```
sudo su -c "source /opt/openstack/swift-kilo/bin/activate && pip install git+https://github.com/openstack/swift@kilo-eol"
```

### Initialize storage space
SAIO introduces 2 types of storage setup

* Using a partition for storage
* Using a loopback device for storage

We don't simulate multiple hosts so we need to use loopback devices in order to play with the replication feature.

Run following commands as a super user.

```
mkdir -p /srv/vdisks
for disk in sdb sdc sdd sde
do
  # Create disk images
  truncate -s 2G /srv/vdisks/$disk
  # Create file systems
  mkfs.xfs /srv/vdisks/$disk
  # Create mount points
  mkdir -p /srv/node/$disk
  mount /srv/vdisks/$disk /srv/node/$disk
  chown swift:swift -R /srv/node/$disk
done
```

Of course, you can run the [script](scripts/init-storage.sh) directly.

```
sudo scripts/init-storage.sh
```

### Configuration

* Configure Swift. We have alreay prepared a set of out-of-box [configuration files](etc/swift/) for you.

```
sudo cp -r $GOPATH/src/github.com/iqiyi/auklet/doc/tiny-swift/etc/swift /etc
```

* Setup dirs

```
sudo mkdir -p /var/cache/swift
sudo chown swift:swift -R /var/cache/swift
sudo mkdir -p /var/run/swift
sudo chown swift:swift -R /var/run/swift
```

* Configure rsync for replicator

```
sudo cp $GOPATH/src/github.com/iqiyi/auklet/doc/tiny-swift/etc/rsyncd.conf /etc/rsyncd.conf
sudo systemctl restart rsyncd
```

### Create rings

```
sudo su -c "source /opt/openstack/swift-kilo/bin/activate && scripts/remakering.sh"
```

# Have fun
By default all the log will be write to `/var/log/messages`.

* Start Swift service

```
sudo su -c "source /opt/openstack/swift-kilo/bin/activate && swift-init start all"
```

* Create a contaienr

```
curl -v -X PUT http://127.0.0.1:8080/v1/iqiyi/auklet
```

* Upload an object

```
curl -v -X PUT -H "Content-Type: text/plain" -d "Hello World" http://127.0.0.1:8080/v1/iqiyi/auklet/hi
```
