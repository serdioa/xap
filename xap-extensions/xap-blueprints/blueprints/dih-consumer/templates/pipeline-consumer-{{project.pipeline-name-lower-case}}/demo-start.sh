#!/usr/bin/env bash
set -e
GS_HOME=${GS_HOME=`(cd ../../../; pwd )`}
echo "GS_HOME = $GS_HOME"
echo "Building consumer..."
./build.sh

echo "Creating container for consumer service (processing unit)..."
$GS_HOME/bin/gs.sh container create --count=1 --zone=pipeline-consumer-{{project.pipeline-name-lower-case}} localhost

echo "Deploying service (processing unit)..."
./deploy.sh

echo "Demo start completed"
