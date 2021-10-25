#!/usr/bin/env bash
echo "Undeploying services (processing units)..."
./undeploy.sh

echo "Killing GSC with zone: pipeline-consumer-{{project.pipeline-name-lower-case}}"
../gs.sh container kill --zone=pipeline-consumer-{{project.pipeline-name-lower-case}}

echo "Demo stop completed"