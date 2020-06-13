#!/bin/bash

set -ex

USER='hellej'

DOCKER_IMAGE=${USER}/hope-graph-updater:dev
DOCKER_IMAGE_LATEST=${USER}/hope-graph-updater:latest

docker build -t ${DOCKER_IMAGE} .

docker tag ${DOCKER_IMAGE} ${DOCKER_IMAGE_LATEST}
docker login
docker push ${DOCKER_IMAGE_LATEST}
