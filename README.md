# SnapDelete

Delete entire subtrees from ZooKeeper by reading from a snapshot file.

## Usage

A typical use case of this tool is when ZooKeeper's database has become too
large due to large number of children of a znode. In this case child znodes
cannot be deleted easily, because `getChildren()` cannot run due to jute buffer 
limitation or computational complexity (DataNode's `addChild()` creates ArrayList
with initial capacity of 9 children). `SnapDelete` tool is trying to get
around the problem by reading the list of znodes from an existing snapshot
file and executes standard delete requests against a running a cluster.

## Installation

Compilee with Maven:

```
mvn clean install
```

Run with the following command:

```
java -cp "./target/lib/*:./target/snapdelete-1.0.jar" org.apache.zookeeper.server.SnapDelete snapshot.17500014df2 127.0.0.1:2181 /large/number/of/children
```

You might want to increase default heap size, because SnapDelete has to
store the list of znodes in memory:

```
java -DXmx50G -Xms50G ...
```

For 50 GB of heap.

## Command line options

```
usage: SnapDelete -[abr] snapshot_file zookeeper_host znode
 -a,--auth <arg>    Digest authentication string (default: noauth)
 -b,--batch <arg>   Batch size (default: 1000)
 -r,--rate <arg>    Rate control, number of simultaneous batches (default:
                    10)
```

`-a,--auth <arg>` Digest authentication string can be set with this argument.
This is the same thing that has to given to `addAuth digest ...` CLI command.
Kerberos authentication is not yet supported.

`-b,--batch <arg>` SnapDelete runs delete operations in multis. Size of each
multi can be controlled with this argument. Default size is 1000.

`-r,--rate <arg>` Rate limit of multis. This number of multis can be in-flight
at any given time. Default is 10.

You can avoid overloading your cluster by the setting the last 2 options 
appropriately.
