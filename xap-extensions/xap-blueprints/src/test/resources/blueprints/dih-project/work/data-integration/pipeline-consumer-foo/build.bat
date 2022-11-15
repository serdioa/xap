@echo off
if exist target (
    echo Purging existing files from target...	
	rd /s /q target
)
call mvn package