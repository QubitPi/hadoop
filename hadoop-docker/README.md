Hadoop Docker Image
===================

[![GitHub Workflow Status]][GitHub Workflow URL]
[![Docker Hub][Docker Pulls Badge]][Docker Hub URL]
[![Apache License Badge]][Apache License, Version 2.0]

![Hadoop Logo](https://user-images.githubusercontent.com/16126939/180641285-1ff7ea05-609a-485a-9f08-519a33571fdb.png)

This image can be used

1. for quick provisioning of real HDFS for Hadoop-related functional tests, and
2. as a base image of other Hadoop-based distributed systems, such as
   [HBase](https://hub.docker.com/r/jack20191124/hbase/).

Specifically, the image offers the following features:

- __Real interactive HDFS__: We could access Hadoop cluster UI, execute all HDFS related commands(e.g.
  `hdfs dfs -ls /`), and run MapReduce jobs just like in a real Hadoop cluster.
- __Easier debugging__: At any point of integration tests, developer could examine the state of the HDFS, which gives 
  them great insights on the code behaviors.
- __Java API__: This image has complete support for integration tests that access HDFS via its Java API. An example 
  snippet is provided below.
- __Support HttpFS__: Wanna talk to HDFS via proxy or something else other than Java API? Not a problem, this images
  opens HttpFS for us~
- __Extensible Image__: Designed as a base image for all Hadoop-based systems, such as
  [HBase](https://hub.docker.com/r/jack20191124/hbase/).

Getting Image
-------------

### Docker Hub

We can pull the image from [my docker hub](https://hub.docker.com/r/jack20191124/hadoop/):

```console
docker pull jack20191124/hadoop:<tag>
```

where `tag` is a regular [Hadoop version](https://archive.apache.org/dist/hadoop/common/). For example, to pull version
3.1.3, use with

```console
docker pull jack20191124/hadoop:3.1.3
```

> [!TIP]
>
> Supported [Hadoop Versions](https://hub.docker.com/r/jack20191124/hadoop/tags) are
>
> - [2.2.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.2.0)
> - [2.3.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.3.0)
> - [2.4.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.4.0), [2.4.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.4.1)
> - [2.5.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.5.0), [2.5.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.5.1), [2.5.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.5.2)
> - [2.6.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.0), [2.6.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.1), [2.6.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.2), [2.6.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.3), [2.6.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.4), [2.6.5](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.6.5)
> - [2.7.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.0), [2.7.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.1), [2.7.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.2), [2.7.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.3), [2.7.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.4), [2.7.5](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.5), [2.7.6](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.6), [2.7.7](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.7.7)
> - [2.8.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.0), [2.8.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.1), [2.8.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.2), [2.8.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.3), [2.8.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.4), [2.8.5](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.8.5)
> - [2.9.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.9.0), [2.9.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.9.1), [2.9.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.9.2)
> - [2.10.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.10.0), [2.10.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.10.1), [2.10.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=2.10.2)
> - [3.0.0-alpha1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0-alpha1), [3.0.0-alpha2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0-alpha2), [3.0.0-alpha3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0-alpha3), [3.0.0-alpha4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0-alpha4), [3.0.0-beta1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0-beta1)
> - [3.0.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.0), [3.0.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.1), [3.0.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.2), [3.0.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.0.3)
> - [3.1.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.1.0), [3.1.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.1.1), [3.1.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.1.2), [3.1.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.1.3), [3.1.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.1.4)
> - [3.2.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.2.0), [3.2.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.2.1), [3.2.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.2.2), [3.2.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.2.3), [3.2.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.2.4)
> - [3.3.0](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.3.0), [3.3.1](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.3.1), [3.3.2](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.3.2), [3.3.3](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.3.3), [3.3.4](https://hub.docker.com/r/jack20191124/hadoop/tags?page=1&name=3.3.4)

### GitHub

We could also build the image from [the source](https://github.com/QubitPi/hadoop/tree/master/hadoop-docker):

```console
git clone git@github.com:QubitPi/hadoop.git
cd hadoop-docker
docker build -t jack20191124/hadoop .
```

To build with a custom Hadoop version, use

```console
docker build -t jack20191124/hadoop --build-arg HADOOP_VERSION=x.y.z .
```

For example, to build a 3.3.0 Hadoop image, execute

```console
docker build -t jack20191124/hadoop --build-arg HADOOP_VERSION=3.3.0 .
```

Starting a Container
--------------------

When image is on our machine (either by pulling or building), we can spin up a HDFS instance in 2 modes:

### Non-Interactive Mode

If we would like to have a HDFS that just runs forever, run

```console
docker run -d --name=hdfs -it \
    -p 8020:8020 \
    -p 50070:50070 \
    -p 50090:50090 \
    -p 50091:50091 \
    -p 50010:50010 \
    -p 50075:50075 \
    -p 50020:50020 \
    -p 14000:14000 \
    jack20191124/hadoop /etc/init.sh -d
```

- __name=hdfs__: the container is named "hdfs". We can change it accordingly.
- __-p 50070:50070__: 50070 is the base port where the dfs namenode web UI will listen on. With this port forwarding,
  we will be able to access namenode web UI from host machine web browser at `localhost:50070`
- __-p 8020:8020 -p 50090:50090 -p 50091:50091 -p 50010:50010 -p 50075:50075 -p 50020:50020__: allow Java API to access 
  HDFS. __This made the container very useful if we are running some integration tests using this image. We are 
  essentially testing against a real HDFS instead of mocked object__
- __-p 14000:14000__: allow host to access HttpFS
- __-d__: two `-d`s(one after `docker run`; one at the end) keep container running in background after start

### Interactive Mode

If we would like to spin up a HDFS and interact with it using shell, run

```console
docker run --name=hdfs -it \
    -p 8020:8020 \
    -p 50070:50070 \
    -p 50090:50090 \
    -p 50091:50091 \
    -p 50010:50010 \
    -p 50075:50075 \
    -p 50020:50020 \
    -p 14000:14000 \
    jack20191124/hadoop /etc/init.sh -bash
```

With interactive mode, we could deploy and run MapReduce Job through shell commands; an example MapReduce job below is 
for us to try out.

> [!NOTE]
>
> When we exit container by 'exit', the container will stop immediately

### NameNode UI

The quickest way to make sure that the container is working properly is by browsing the web interface for the NameNode
which, by default, is available at http://localhost:50070/

### Executing an Example MapReduce Job

Jumping into the container, we can play with this example. Make the HDFS directories required to execute MapReduce jobs:

```console
hdfs dfs -mkdir /user
hdfs dfs -mkdir -p /user/root
```

Copy the input files into the distributed filesystem:

```console
hdfs dfs -put $HADOOP_HOME/etc/hadoop input
```

Run the examples provided:

```console
$HADOOP_HOME/bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-<version>.jar grep input output 'dfs[a-z.]+'
```
    
Note that `<version>` is the version of hadoop, e.g. `2.8.4`.

Examine the output files: Copy the output files from the distributed filesystem to the local filesystem and examine
them:

```console
hdfs dfs -get output output
cat output/*
```

or, view the output files on the distributed filesystem:

```console
hdfs dfs -cat output/*
```

JVM API
-------

Let's say we wrote a HDFS client and would like to test it. Our client passed unit tests via mocking. Now we can test
the client against a real HDFS during integration tests with this image. Here is a Groovy example:

<img width="920" alt="jvm-api" src="https://user-images.githubusercontent.com/16126939/179653433-91bcda63-e5cc-4669-bb24-3ce764643c15.png">

This images runs container as `root` user. We should always send API calls as root user. User-spaced integration tests 
might be supported in future release.

`hdfs://localhost:8020/` means we, as host, are accessing HDFS from localhost at port 8020. The port forwarding of `-p 
8020:8020` maps `localhost:8020` to `container:8020`. We can always use the same `localhost:8020` in integration tests. 
This is a consistency advantage that this images gives to us.

HttpFS
------

Our team might have proxy in front of Hadoop cluster and we shall access HDFS via proxy's REST HTTP endpoints. This 
image allows us to write integration tests for this situation.

To demonstrate, spinup a container in interactive mode and create some files by executing the following commands in the 
container:

```console
hdfs dfs -mkdir -p /user/root
hdfs dfs -put $HADOOP_HOME/etc/hadoop input
```


Now we have some files in HDFS under some path. Our integration tests can access them via `curl` or HTTP API in our 
programming language. Here is how we do it via `curl`:

```console
curl "http://localhost:14000/webhdfs/v1/user/root/input/core-site.xml?op=open&user.name=root"
```

Note that we access the files from our host machine because that's where we integration tests are ;)

License
-------

The use and distribution terms for [Hadoop Docker][Docker Hub URL] are covered by the [Apache License, Version 2.0].

[Apache License Badge]: https://img.shields.io/badge/Apache%202.0-FE5D26.svg?style=for-the-badge&logo=Apache&logoColor=white
[Apache License, Version 2.0]: https://www.apache.org/licenses/LICENSE-2.0

[Docker Pulls Badge]: https://img.shields.io/docker/pulls/jack20191124/hadoop?style=for-the-badge&logo=docker&logoColor=white&labelColor=5BBCFF&color=7EA1FF
[Docker Hub URL]: https://hub.docker.com/r/jack20191124/hadoop

[GitHub Workflow Status]: https://img.shields.io/github/actions/workflow/status/QubitPi/hadoop/docker.yaml?branch=master&logo=github&style=for-the-badge&label=CI/CD&labelColor=2088FF
[GitHub Workflow URL]: https://github.com/QubitPi/hadoop/actions/workflows/docker.yaml
