@echo off

echo Building consumer...
call build.bat

echo Creating container for space service (processing unit)...
call ..\gs container create --count=1 --zone=pipeline-consumer-{{project.pipeline-name-lower-case}} localhost

echo Deploying service (processing unit)...
call deploy.bat

echo Demo start completed