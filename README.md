Hadoop
======

[![Docker Hub][Docker Pulls Badge]][Docker Hub URL]

Deploying [Hadoop Documentation][documentation] to GitHub Pages
---------------------------------------------------------------

```bash
git clone https://github.com/QubitPi/hadoop.git && cd hadoop && ./start-build-env.sh
mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true && mvn site site:stage -Preleasedocs,docs -DstagingDirectory=/tmp/hadoop-site
```

Building Hadoop from Source
---------------------------

### Prepare the Build Environment

The first thing we will do is to `git clone` the Apache Hadoop repository:

```bash
git clone https://github.com/QubitPi/hadoop.git && cd hadoop
```

Notice the [start-build-env.sh](./start-build-env.sh) file at the root of the project. It is a very convenient script
that builds and runs a Docker container in which everything needed for building and testing Hadoop is included. The
[Docker image](./dev-support/docker/Dockerfile) is based on Ubuntu 18.04. Having an "official" building container is a 
really great addition to any open source project, it helps both new developers on their journey to a first contribution
as well as maintainers to reproduce issues more easily by providing a controlled and reproducible environment.

At the end of the `start-build-env.sh` script, the container is started with the following properties:

```bash
docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${PWD}:${DOCKER_HOME_DIR}/hadoop${V_OPTS:-}" \
  -w "${DOCKER_HOME_DIR}/hadoop" \
  -v "${HOME}/.m2:${DOCKER_HOME_DIR}/.m2${V_OPTS:-}" \
  -v "${HOME}/.gnupg:${DOCKER_HOME_DIR}/.gnupg${V_OPTS:-}" \
  -u "${USER_ID}" \
  "hadoop-build-${USER_ID}" "$@"
```

### Building Hadoop without Running the Tests

The [BUILDING.txt](./BUILDING.txt) file at the root of the project gives us instruction about building commands with
Maven as examples:

```bash
# Create binary distribution without native code and without documentation:
mvn install -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true
```

- The `-DskipTests` parameter, as the name suggests makes a build without running the unit tests
- `-Pdist` and `-Dtar` are the parameters we use to produce a distribution with a .tar.gz file extension like the one we 
  obtain from downloading the latest build on the Apache Hadoop release page.
- `-Dmaven.javadoc.skip=true` is there to exclude the documentation from the build.

These options contribute to speed up the build process. After a few minutes, here is the output of the command:

```bash
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  12:54 min
[INFO] Finished at: 2020-07-21T12:41:50Z
[INFO] ------------------------------------------------------------------------
```

The building and packaging of the distribution is done but where is our .tar.gz file? It is located in the
**hadoop-dist** (for Hadoop distribution assembler) maven module under the target folder:

```bash
➜ hadoop git:(aa96f1871bf) ✗ ls -alh ./hadoop-dist/target/hadoop-3.3.0.tar.gz
-rw-r--r-- 1 leo leo 431M Jul 27 14:42 ./hadoop-dist/target/hadoop-3.3.0.tar.gz
```

The .tar.gz is also available outside of the docker container because the **Hadoop source directory was mounted in the
`docker run` command**, i.e. `-v "${PWD}:${DOCKER_HOME_DIR}/hadoop${V_OPTS:-}"``.

### Running Unit Tests

It is critical to make sure the tests are running properly before making our release. Using the docker image provided
above to run the tests, we can build with the test running using:

```bash
mvn install -Pdist -Dtar -Dmaven.javadoc.skip=true
```

Hadoop is a big and complex project. Therefore, it is split into multiple maven modules as described in the
[BUILDING.txt](./BUILDING.txt) file of the repository. For efficiency's sake, we can choose to run only the tests of some
particular module, such as `hadoop-hdfs-project` which contains the core code for its components such as the Namenode,
the Datanode, etc. There are more than 700 unit tests defined in this submodule only.

```bash
cd hadoop-hdfs-project && mvn package -Pdist -Dtar -Dmaven.javadoc.skip=true
```

[Docker Pulls Badge]: https://img.shields.io/docker/pulls/jack20191124/kugelblitz?style=for-the-badge&logo=docker&logoColor=white&labelColor=5BBCFF&color=7EA1FF
[Docker Hub URL]: https://hub.docker.com/r/jack20191124/hadoop
[documentation]: https://hadoop.qubitpi.org/
