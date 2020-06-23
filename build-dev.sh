#!/bin/bash

set -ex

USER='hellej'

DOCKER_IMAGE=${USER}/hope-graph-updater

docker build -t ${DOCKER_IMAGE}:dev -t ${DOCKER_IMAGE}:latest .

for TAG in dev latest; do
  docker push ${DOCKER_IMAGE}:${TAG}
done