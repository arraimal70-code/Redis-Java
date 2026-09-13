@echo off
echo ===================================================
echo  Starting Custom Redis Server on Port 6379
echo ===================================================

"C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot\bin\java.exe" -cp "bin" com.redisclone.server.RedisServer %*
