#!/usr/bin/env bash
if [ -e target ]; then
    echo "Purging existing files from target..."
    rm -r target
fi
mvn package