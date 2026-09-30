# Deploying the DTS via Docker

You can use the `Dockerfile` and `dts.yaml` files in the `deployment` folder to
build a Docker image for the Data Transfer System (DTS). The Docker image
contains two files:

1. `/bin/dts`: the statically-linked `dts` executable
2. `/etc/dts.yaml`: a [DTS configuration file](config.md) with embedded
   environment variables that control parameters of interest

This image can be deployed in any Docker-friendly environment. The use of
environment variables in the configuration file allows you to configure the
DTS without regenerating the image.

## Deploying to NERSC's Spin Environment

The "primary instance" of the DTS is hosted in NERSC's [Spin](https://www.nersc.gov/systems/spin/)
environment under [Rancher 2](https://rancher2.spin.nersc.gov/login).
It runs in the `Production` environment under the `kbase` organization.
You can read about Spin in NERSC's documentation, and Rancher 2
[here](https://rancher.com/docs/rancher/v2.x/en/). The documentation
isn't great, but fortunately there's not a lot to know--most of the
materials you'll need are right here in the `deployment` folder.

Deploying the DTS to Spin involves

1. updating and pushing a new Docker image with any code changes and
   documentation updates
2. editing the `dts` Spin deployment via NERSC's
   [Rancher 2](https://rancher2.spin.nersc.gov/login) console

Each of these steps are fairly simple.

Before you perform an update, take some time to familiarize yourself
with the Rancher 2 console and the `dts` production deployment.
The most important details are:

* The service and its supporting executables and configuration data are
  supplied by its Docker image
* Configurable settings for the service are stored in environment variables
  that can be set in the Rancher 2 console
* The DTS data directory (used for keeping track of ongoing tasks and for
  generating transfer manifests) resides on the NERSC Community File System
  (CFS) under `/global/cfs/cdirs/kbase/dts/`. This volume is visible to the
  service as `/data`, so the `DATA_DIRECTORY` environment variable should be
  set to `/data`.
* The DTS manifest directory (used for writing transfer manifest files that
  get transferred to destination endpoints) also resides on the NERSC
  Community File System (CFS), but under `/global/cfs/cdirs/kbase/gsharing/dts/manifests`
  so that it is accessible via a Globus endpoint. This volume is visible to
  the service as `/manifests`, so the `MANIFEST_DIRECTORY` environment variable
  should be set to `/manifests`. **NOTE: the directory must be the same when
  viewed by the service and the Globus Collection! If there is a mismatch,
  the service will not be able to write the manifest OR Globus will not be
  able to transfer it.**

Let's walk through the process of updating and redeploying the DTS in Spin.

### 1. Update and Push a New Docker Image to Spin

From within a clone of the [DTS GitHub repo](https://github.com/kbase/dts), make
sure the repo is up to date by typing `git pull` in the `main` branch.

Then, sitting in the top-level `dts` source folder of your `dts`, execute
the `deploy-to-spin.sh` script, passing as arguments

1. the name of a tag to identify the new Docker image
2. the name of the NERSC user whose permissions are used for CFS
3. the UID of the NERSC user
4. the group used to determine the user's group permissions
5. the GID of the above group

For example,

```
./deployment/deploy-to-spin.sh v1.1 johnson 52710 kbase 54643
```

builds a new DTS Docker image for to be run as the user `johnson`,
with the tag `v1.1`. The script pushes the Docker image to [Harbor, the
NERSC Docker registry](https://registry.nersc.gov). Make sure the tag
indicates the current version of `dts` (e.g. `v1.1`) for clarity.

After building the Docker image and tagging it, the script prompts you for the
NERSC password for the user you specified. This allows it to push the image to
Harbor so it can be accessed via the Rancher 2 console.

### 2. Edit the Deployment in Rancher 2 and Restart the Service

Now log in to [Rancher 2](https://rancher2.spin.nersc.gov/login) and
navigate to the `dts` deployment.

1. Click on the `dts` pod to view its status and information
2. Click on the three dots near the right side of the screen and select
   `Edit` to update its configuration.
3. If needed, navigate to the `Volumes` section and edit the CFS directory for
   the volume mounted at `/data`. Usually, this is set to `/global/cfs/cdirs/kbase/dts/`,
   so you usually don't need to edit this. Similarly, check the volume mounted
   at `/manifests` (usually set to `/global/cfs/cdirs/kbase/gsharing/dts/manifests/`).
4. Edit the Docker image for the deployment, changing the tag after the colon
   to match the tag of the Docker image pushed by `deploy-to-spin.sh` above.
5. Make sure that the Scaling/Upgrade Policy on the Deployment is set to
   `Recreate: KILL ALL pods, then start new pods.` This ensures that the
   service in the existing pod can save a record of its ongoing tasks before a
   service in a new pod tries to restore them.
6. Click `Save` to restart the deployment with this new information.

That's it! You've now updated the service with new features and bugfixes.

## Maintaining the DTS Data Directory

The DTS uses its data directory to manage its own state, as well as its
interactions with other services. Currently, the data directory contains the
following files:

* `dts.gob` - a file containing information about pending and recently finished
  file transfers, along with any related database-specific state information
* `kbase_user_orcids.csv` - a comma-separated variable file associating ORCID
  identifiers with KBase users, and optionally with Globus IDs (which are UUIDs).
  This file is a temporary mechanism that allows the DTS to obtain the username
  and Globus ID for a KBase user given their ORCID. It is re-read at the top of
  the hour, making it easy to replace without restarting a deployment.
