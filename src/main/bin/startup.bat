@echo off
@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT"  setlocal

set ENV_PATH=.\
if "%OS%" == "Windows_NT" set ENV_PATH=%~dp0%

set conf=%ENV_PATH%\..\conf
set yugong_conf=%conf%\yugong.properties
set logback_configurationFile=%conf%\logback.xml
set classpath=%conf%\..\lib\*;%conf%

set JAVA_OPTS= -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dapplication.codeset=UTF-8 -Dfile.encoding=UTF-8 -Xms128m -Xmx512m -XX:PermSize=128m -XX:+HeapDumpOnOutOfMemoryError -DappName=yugong -Dlogback.configurationFile="%logback_configurationFile%" -Dyugong.conf="%yugong_conf%"
set JAVA_DEBUG_OPT= -server -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=9099,server=y,suspend=n

set COMMAND= java %JAVA_OPTS% %JAVA_DEBUG_OPT% -classpath "%classpath%" com.taobao.yugong.YuGongLauncher
echo %COMMAND%
java %JAVA_OPTS% %JAVA_DEBUG_OPT% -classpath "%classpath%" com.taobao.yugong.YuGongLauncher