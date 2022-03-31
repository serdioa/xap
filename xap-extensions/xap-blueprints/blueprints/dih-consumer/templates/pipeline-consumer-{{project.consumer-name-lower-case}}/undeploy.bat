@echo off

SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..\..
call %GS_HOME%\bin\gs pu undeploy pipeline-consumer-{{project.consumer-name-lower-case}}