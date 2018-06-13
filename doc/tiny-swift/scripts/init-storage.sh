#!/usr/bin/env bash

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
