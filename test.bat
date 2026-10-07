@echo off
echo ===================================================
echo  Running Custom Redis Architecture Test Harness
echo ===================================================

java -cp "bin" com.redisclone.RedisServerTest
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

java -cp "bin" com.redisclone.FailureAndEdgeCaseTest
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

java -cp "bin" com.redisclone.AdversarialTest
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

echo [SUCCESS] All test suites passed successfully!
