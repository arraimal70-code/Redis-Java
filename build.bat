@echo off
echo ===================================================
echo  Compiling Custom Redis Clone (Core Java 21)
echo ===================================================

if not exist bin mkdir bin

dir /s /b src\*.java test\*.java > sources.txt
"C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot\bin\javac.exe" -d bin -cp "bin" @sources.txt
del sources.txt

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed!
    exit /b %ERRORLEVEL%
)

echo [SUCCESS] Compilation completed into .\bin
