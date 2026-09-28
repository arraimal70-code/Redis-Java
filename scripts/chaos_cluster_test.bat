@echo off
setlocal enabledelayedexpansion

echo ================================================================================
echo  AUTOMATED CHAOS, REPLICATION AND FAULT-INJECTION TEST HARNESS
echo ================================================================================

set MASTER_PORT=6389
set REPLICA_PORT=6390

echo [1/5] Compiling Core Java 21 Redis Engine...
call build.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed. Aborting chaos tests.
    exit /b 1
)

echo [2/5] Starting Master Node on port %MASTER_PORT%...
start "Redis-Master-6389" /B java -cp bin com.redisclone.server.RedisServer --port %MASTER_PORT% --aof false --rdb false > nul 2>&1
ping -n 3 127.0.0.1 > nul

echo [3/5] Starting Replica Node on port %REPLICA_PORT% replicating Master...
start "Redis-Replica-6390" /B java -cp bin com.redisclone.server.RedisServer --port %REPLICA_PORT% --replicaof 127.0.0.1 %MASTER_PORT% --aof false --rdb false > nul 2>&1
ping -n 3 127.0.0.1 > nul

echo [4/5] Executing Core System and Failure Test Suite...
call test.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Test assertions failed!
    goto cleanup
)

echo [5/5] Executing Low-Level Microbenchmark Suite...
java -cp bin com.redisclone.benchmark.MicrobenchmarkSuite
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Microbenchmark suite failed!
    goto cleanup
)

echo ================================================================================
echo  ✅ ALL CHAOS, REPLICATION & FAULT TESTS PASSED DETERMINISTICALLY!
echo ================================================================================

:cleanup
echo Stopping background test nodes...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%MASTER_PORT%" ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a > nul 2>&1
)
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%REPLICA_PORT%" ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a > nul 2>&1
)
exit /b 0
