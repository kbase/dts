# This is the DTS Docker file. It's a multi-ѕtage build that results in a
# minimal-esque image with a small number of files:
#
# 1. /bin/dts (statically linked)
# 2. /etc/dts.yaml
#
# Additionally, some useful operating system utilities are provided.
#
# The service runs on a port determined by the PORT environment variable. You
# can map this port to any port on the host using the -p flag of the
# `docker run` command.
#
# ---------------
# BUILD ARGUMENTS
# ---------------
#
# You can provide a username/UID pair and a group name/GID pair that identify
# a user/group on the host system that confer the privileges for the service
# to operate properly (e.g. file system access). Specify each of these as build
# arguments to `docker build` with `--build-arg NAME=VAL`.
#
# The following build variables are used when the Docker image is being built:
# CONTACT_NAME: The name of the point of contact for the service
# CONTACT_EMAIL: The email address for the point of contact
# SERVER_URL: The URL for the running service (with slashes escaped)
# TERMS_OF_SERVICE: The URL at which the terms of service may be retrieved (with
#                   slashes escaped)
#
# The following build variables are used as environment variables during
# run-time:
# USERNAME: The name of the (non-root) user that runs the service
#           (default: gsuser)
# UID: The ID corresonding to the user on the host system (default: $UID)
# GROUP: The name of the group to which the user belongs while running the
#        service (default: gsuser)
# GID: The ID corresonding to the group on the host system (default: $UID)
#
# All of these build arguments have defaults in case they are not needed.
#
# ------------------
# CONFIGURATION FILE
# ------------------
#
# The docker build process assumes that there exists a configuration file
# named dts.yaml in the deployment/ directory of the project.
# This file contains the configuration that the Docker image uses to run the
# service.
#
# ---------------------
# ENVIRONMENT VARIABLES
# ---------------------
#
# The docker image assumes that certain environment variables are defined at
# run-time. See `dts.yaml` for a list of these variables and an illustration
# of how they are used.
#
# ------------------
# BUILDING THE IMAGE
# ------------------
#
# To build the image, issue the following commands from the top-level
# directory:
#
# 1. docker build --network=host [--build-arg ARG1=VAL1 ...] -t dts .
# 2. docker image prune --filter label=stage=builder
#
# The first command builds the image using an intermediate "builder" image.
# The second command deletes the builder image.

#-------------------------------------------------------
#           Dockerfile content starts here
#-------------------------------------------------------

# Here's a reasonable Go-equipped Docker image.
FROM golang:1.22-alpine AS builder

# Process build arguments and pass them along to the environment.
ARG CONTACT_NAME=Unspecified
ARG CONTACT_EMAIL=dts@example.com
ARG TERMS_OF_SERVICE_URL=http://example.com
ARG SERVER_URL=http://example.com

#-------------------------------------------------------
# Build the service executable.
#-------------------------------------------------------
# We label this image as "builder" so we can prune it afterward with
# docker image prune --filter label=stage=builder
LABEL stage=builder
# Fetch some essentials.
RUN apk update && apk add --no-cache git build-base
# Set the directory we work in within the image.
WORKDIR /go/dts
# Copy files from our context to the working directory.
COPY . .
# Build dts as a static executable.
RUN env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a

#--------------------------------------------------------
# Now we create a smaller image containing only
# 1. the service executable
# 2. a generated configuration file (dts.yaml)
# 3. other UNIX stuff provided by the `alpine` base image
#--------------------------------------------------------

# Here's the alpine Docker container. If you ever want to peek at its contents,
# type `docker run -it alpine /bin/ls`.
FROM alpine:3.20

# Process environment variable build arguments
ARG USERNAME=gsuser
ENV USERNAME=$USERNAME
ARG UID=1000
ENV UID=$UID
ARG GROUP=gsgroup
ENV GROUP=$GROUP
ARG GID=1001
ENV GID=$GID

# Copy essential stuff from the builder image.
COPY --from=builder /go/dts/dts /bin/dts
COPY --from=builder /go/dts/deployment/dts.yaml /etc/dts.yaml

ENV PATH=/bin:/usr/bin:/usr/sbin

# Add the user and group for this image and set it up.
RUN /usr/sbin/adduser -u $UID -H -D $USERNAME && \
    /usr/sbin/addgroup -g $GID $GROUP && \
    /usr/sbin/addgroup $USERNAME $GROUP
USER $USERNAME

# Run the service on the given port with the given config file.
ENTRYPOINT ["/bin/dts"]
CMD ["/etc/dts.yaml"]
