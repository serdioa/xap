#!/usr/bin/env bash
set -e

echo "Building consumer..."
./build.sh

echo "Creating container for consumer service (processing unit)..."
../gs.sh container create --count=1 --zone=pipeline-consumer-{{project.pipeline-name-lower-case}} localhost

echo "Deploying service (processing unit)..."
./deploy.sh

echo "Demo start completed"
