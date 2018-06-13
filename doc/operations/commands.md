# Builtin Management commands
Auklet comes with some management commands. It is also possible to manage services with systemd.

### Object Server
* Start object server: `auklet start object -l /etc/swift/zap.yml`
* Stop object gracefully: `auklet shutdown object`
* Restart object server gracefully: `auklet reload object`
* Stop object server forcely(Don't use this command unless you have to): `auklet stop object`
* Restart object server forcely(Don't use this command unless you have to): `auklet restart object`

### Pack Replicator
* Start pack replicator as daemon: `auklet start pack-replicator`
* Start pack replicator for only one pass: `auklet start pack-replicator -once`
* Only replicate disk sdb: `auklet start pack-replicator -devices sdb`
* Only replicate partition 12: `auklet start pack-replicator -partitions 12`

### Pack Auditor
* Start pack auditor as daemon: `auklet start pack-auditor`
* Start pack auditor for only one pass: `auklet start pack-auditor -once`
* Only audit disk sdb: `auklet start pack-auditor -devices sdb`
* Only audit partition 12: `auklet start pack-auditor -partitions 12`

# Systemd
One advantage to use systemd to manage service is that panic service  could be launched automatically. 

NOTE: Don't use systemd and Auklet's management commands simultaneously. You should adhere to only one way.

We have already provides a [systemctl wrapper example](../../packages/rpm/auklet-object.service) for object server. Wrappers for replicator/auditor are planed to be added later.

* Start object server: `systemctl start auklet-object`
* Stop object server: `systemctl stop auklet-object`
* Restart object server: `systemctl restart auklet-object`
