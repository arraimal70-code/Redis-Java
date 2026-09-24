@echo off
echo ===================================================
echo  Compiling Custom Redis Clone (Core Java 21)
echo ===================================================

if not exist bin mkdir bin

powershell -NoProfile -Command "Get-ChildItem -Recurse -Include *.java src, test, examples -ErrorAction SilentlyContinue | ForEach-Object { '\"' + $_.FullName.Replace('\', '/') + '\"' } | Out-File -Encoding ascii sources.txt"
"C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot\bin\javac.exe" -d bin -cp "bin" @sources.txt
if exist sources.txt del sources.txt

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed!
    exit /b %ERRORLEVEL%
)

echo [SUCCESS] Compilation completed into .\bin
