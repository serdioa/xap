#!/usr/bin/env bash
echo "Undeploying services (processing units)..."
./undeploy.sh

echo "Killing GSC with zone: pipeline-consumer-{{project.consumer-name-lower-case}}"
../gs.sh container kill --zone=pipeline-consumer-{{project.consumer-name-lower-case}}

echo "Demo stop completed"