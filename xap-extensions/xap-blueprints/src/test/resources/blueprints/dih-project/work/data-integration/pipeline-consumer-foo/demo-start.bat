@echo off

echo Building consumer...
call build.bat

echo Creating container for space service (processing unit)...
SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..\..
call %GS_HOME%\bin\gs container create --count=1 --zone=pipeline-consumer-foo localhost

echo Deploying service (processing unit)...
call deploy.bat

echo Demo start completed