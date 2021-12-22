#!/usr/bin/env bash
echo "Undeploying services (processing units)..."
./undeploy.sh

echo "Killing GSC with zone: pipeline-consumer-foo"
../gs.sh container kill --zone=pipeline-consumer-foo

echo "Demo stop completed"