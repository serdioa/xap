@echo off
echo Undeploying services (processing units)...
call undeploy.bat

echo Killing GSC with zone: pipeline-consumer-{{project.pipeline-name-lower-case}}
call ..\gs container kill --zone=pipeline-consumer-{{project.pipeline-name-lower-case}}

echo Demo stop completed
pause