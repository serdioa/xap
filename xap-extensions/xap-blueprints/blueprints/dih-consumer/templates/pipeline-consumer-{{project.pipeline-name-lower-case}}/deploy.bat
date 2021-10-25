@echo off
SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..\..
call %GS_HOME%\bin\gs pu deploy pipeline-consumer-{{project.pipeline-name-lower-case}} dih-consumer\target\{{project.pipeline-name-lower-case}}-dih-consumer.war