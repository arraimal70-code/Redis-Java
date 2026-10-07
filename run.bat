@echo off
echo ===================================================
echo  Starting Custom Redis Server on Port 6379
echo ===================================================

java -cp "bin" com.redisclone.server.RedisServer %*
