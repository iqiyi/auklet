#!/usr/bin/env bash

for disk in sdb sdc sdd sde
do
  umount /srv/node/$disk
  mkfs.xfs -f /srv/vdisks/$disk
  mount /srv/vdisks/$disk /srv/node/$disk
  chown vagrant:vagrant -R /srv/node/$disk
done
