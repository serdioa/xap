@echo off
SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..\..
call %GS_HOME%\bin\gs pu deploy pipeline-consumer-{{project.consumer-name-lower-case}} dih-consumer\target\{{project.consumer-name-lower-case}}-dih-consumer.war