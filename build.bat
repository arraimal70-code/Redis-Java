@echo off
echo ===================================================
echo  Compiling Custom Redis Clone (Core Java 21)
echo ===================================================

if not exist bin mkdir bin

powershell -NoProfile -Command "Get-ChildItem -Recurse -Include *.java src, test, examples -ErrorAction SilentlyContinue | ForEach-Object { '\"' + $_.FullName.Replace('\', '/') + '\"' } | Out-File -Encoding ascii sources.txt"
javac -d bin -cp "bin" @sources.txt
set BUILD_ERR=%ERRORLEVEL%
if exist sources.txt del sources.txt

if %BUILD_ERR% NEQ 0 (
    echo [ERROR] Build failed!
    exit /b %BUILD_ERR%
)

echo [SUCCESS] Compilation completed into .\bin
