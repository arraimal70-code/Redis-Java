@echo off
echo ===================================================
echo  Running Custom Redis Architecture Test Harness
echo ===================================================

"C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot\bin\java.exe" -cp "bin" com.redisclone.RedisServerTest
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

"C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot\bin\java.exe" -cp "bin" com.redisclone.FailureAndEdgeCaseTest
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
