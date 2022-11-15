@echo off
echo Undeploying services (processing units)...
call undeploy.bat

echo Killing GSC with zone: pipeline-consumer-foo
call ..\gs container kill --zone=pipeline-consumer-foo

echo Demo stop completed
pause