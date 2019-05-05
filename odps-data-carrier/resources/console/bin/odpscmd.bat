@echo off
set argC=0
for %%x in (%*) do Set /A argC+=1

SETLOCAL ENABLEDELAYEDEXPANSION
@set basedir="%~dp0"
pushd %basedir%

@set mainclass=com.aliyun.openservices.odps.console.ODPSConsole
@set libpath=%cd%\..\lib\
@set mrlibpath=%cd%\..\lib\mapreduce-api.jar
@set confpath=%cd%\..\conf\
@set classpath=.;!confpath!;!mrlibpath!;!libpath!\*

@set java9opt=--add-modules=java.xml.bind
java -version 2>&1 | findstr /c:"build 1." > nul
if %errorlevel% == 0 @set java9opt=

rem set java env
popd
java -Xms64m -Xmx512m %java9opt% -classpath "!classpath!" %mainclass% %*

if errorlevel 1 (
  if %argC% == 0 (
   PAUSE
  )
)

ENDLOCAL
