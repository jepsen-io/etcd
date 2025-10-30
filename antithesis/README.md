# Jepsen Etcd Antithesis Tests

This is a pile of hacks to try and get a Jepsen test for etcd running inside
Antithesis.

## Getting a build environment

You'll need two files with Antithesis credentials. The first is `gar.key.json`,
which has the auth information for Antithesis' Docker repository. That should
look like:

```json
{"type": "service_account",
 "project_id": "foo-bar-123",
 ...}
```

You also need a shell script with environment variables for talking to the Antithesis API. Create a file called `antithesis_env` with:

```bash
# Your username for API authentication
export ANTITHESIS_USER="..."
# Your password for API authentication
export ANTITHESIS_PASSWORD="..."
# The tenant used for the docker repository
export ANTITHESIS_TENANT="$ANTITHESIS_USER"
# The HTTP endpoint path fragment used to launch new tests
export ANTITHESIS_ENDPOINT="etcd"
```

Now we need a build machine to build Docker containers. If you already have
docker-compose, you can use that locally. If you're aphyr, and you can't let
Docker touch your machine because it'll break other container systems and
destroy your iptables config, launch an AWS Debian instance. An m8g.2xlarge
should do it, if you want to run tests locally. If you don't have enough
memory, Docker containers will randomly exit and it will be exceedingly
frustrating trying to figure out why.

```
jepsen-aws instance start
sudo apt update
```

## Building

If you're @aphyr, sync the top-level `jepsen.etcd` dir to the EC2 instance. If
you have Docker locally, you can skip this.

```
jepsen-aws instance sync -d jepsen.etcd
```

To build, push, and launch a test, run this on the build machine:

```
cd ~/atb; ./run
```

## Step by step

The `./run` script does the following steps, which you might want to run
manually. First, build images:

```
./build
```

Start cluster to test locally.

```
./test
```

Push the images to Antithesis.

```
./push
```

Locally, load API credentials and fire off the request to run the test.

```
./launch
```
