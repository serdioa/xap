@echo off
SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..\..
call %GS_HOME%\bin\gs pu deploy pipeline-consumer-foo dih-consumer\target\foo-dih-consumer.war