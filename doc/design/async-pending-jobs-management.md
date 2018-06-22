### Introduction
Async pending job is unfmailiar to most Swift users because it is internal mechanism of Swift to achieve eventual consistency of container list. OK, so what does that mean ?

After an object is saved to Swift, for example by `curl -v -X PUT -H "Content-Type: text/plain" -d "Hello World" http://127.0.0.1:8080/v1/iqiyi/auklet/hi`, the entries under container `auklet` could be listed by `curl http://127.0.0.1:8080/v1/iqiyi/auklet`. Objects themselves and content of containers are actually managed separately. Swift will firstly save the objects to disk and then send another request to container servers to update the content of containers. But what happen if the object servers works but container servers are down? In that cases, the PUT requests will be considered as successful and the requests will be persisted in disks in order to be retried by another Swift interal service called `object-updater` laterly.

Besides the case mentioned above, there is another important one, namely the [object expiration](https://docs.openstack.org/ocata/user-guide/cli-swift-set-object-expiration.html). When an object is expired, it will not be deleted from disks immediately. It is `swift-object-expirer` that deletes the objects finally. But how does expirer knows which objects to be deleted? It is impractice to list all the objects in the cluster to check if any one has been expired. So Swift will create entries in containers under the `.expiring_objects` account for expiring objects. So expirer could list all the entries under containers within the `.expiring_objects` account to know which objects need to be deleted. All those virtual object entries will be sent container servers by `object-updater` in the form of async pending jobs.


### Pending Job Management in Swift
In Swift, each pending job is serialized with pickle and saved as a standalone file under `/srv/node/sdb/async_pending`. `object-updater` will list files under it and deserialize the content. This design is simple, however, there are some disadvantages.

* Space waste. Each file is typically several hundreds bytes, however it requires 4K file system space.
* Performance degradation. If there are hundreds of millions async job files, it would be slow to list all the files. It unlikely the containers server down for long time, but if there are tons of expiring objects, that would be a serious problem.


### Pending Job Management in Auklet
To address the problem mentioned above, we try to improve the situation with following design.

* Async job will be saved to RocksDB
* To reduce the migration effort, the new solution will be fully compatible with the Swift one.

By default, Auklet will use the same mechanism used in Swift. The new manager must be enabled explicitly by the configuration.

```
[app:object-server]
async_job_manager = kv
```

To be compatible with existing pending job files, another choice must be turned on.

```
[app:object-server]
async_kv_fs_compatible = yes
```

In summary, the new manager has following features.

* Save jobs into RocksDB
* Fully compatible with legacy Swift pending job file format
* By default, everything work as before
