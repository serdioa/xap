#!/usr/bin/env bash
set -e
GS_HOME=${GS_HOME=`(cd ../../../; pwd )`}
$GS_HOME/bin/gs.sh pu deploy pipeline-consumer-foo dih-consumer/target/foo-dih-consumer.war