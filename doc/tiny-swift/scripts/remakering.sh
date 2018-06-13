#!/usr/bin/env bash

cd /etc/swift

rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

swift-ring-builder object.builder create 8 3 1
swift-ring-builder object.builder add r1z1-127.0.0.1:6000/sdb 1
swift-ring-builder object.builder add r1z2-127.0.0.1:6000/sdc 1
swift-ring-builder object.builder add r1z3-127.0.0.1:6000/sdd 1
swift-ring-builder object.builder add r1z4-127.0.0.1:6000/sde 1
swift-ring-builder object.builder rebalance

swift-ring-builder container.builder create 8 3 1
swift-ring-builder container.builder add r1z1-127.0.0.1:6001/sdb 1
swift-ring-builder container.builder add r1z2-127.0.0.1:6001/sdc 1
swift-ring-builder container.builder add r1z3-127.0.0.1:6001/sdd 1
swift-ring-builder container.builder add r1z4-127.0.0.1:6001/sde 1
swift-ring-builder container.builder rebalance

swift-ring-builder account.builder create 8 3 1
swift-ring-builder account.builder add r1z1-127.0.0.1:6002/sdb 1
swift-ring-builder account.builder add r1z2-127.0.0.1:6002/sdc 1
swift-ring-builder account.builder add r1z3-127.0.0.1:6002/sdd 1
swift-ring-builder account.builder add r1z4-127.0.0.1:6002/sde 1
swift-ring-builder account.builder rebalance
