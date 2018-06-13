### Log configuration
Auklet use [Zap](https://github.com/uber-go/zap) as the logging framework. Loggers are configured from configure files. Refer to the [sample](../../etc/zap.yml) for more detail.

You can introduce Auklet to find the configuration file by the `-l` command line argument. Object server, replicator and auditor all support the arguments.

### Pack Replicator
Like Swift object replicator, pack replicator also uses `object-replicator` section. `concurrency` controls how many disks could be replicated concurrent.

```
[object-replicator]
concurrency = 2
```

### Pack Auditor
Like Swift object auditor, pack auditor also uses `object-auditor` section. 
* `concurrency` controls how many disks could be audited concurrent.
* `files_per_second` limits how many files could be audited at most per second
* `bytes_per_second` limits how many bytes could be audited at most per second

```
[object-auditor]
files_per_second = 20
concurrency = 1
bytes_per_second = 5000000
```

### Pack Engine
* `lazy_migration` controls whether to enable lazy migration or not. Note, we have not run that in production environment.
* `test_mode` means there is no need to use a mounted file system as the device, designed for unit test, so ignore it in production environment.
* `pack_chunked_object` controls whether to put objects whose size is unknown at first into the bundle file or not. In HTTP protocol, it is impossible to know the exact size of object if it is sent by `chunked-encoding`. If this option is disabled, then objects sent by `chunked-encoding` will be save as standalone files like replication engine, otherwise it would be save into bundle file.

```
[object-pack]
lazy_migration = no
test_mode = no
pack_chunked_object = no
```