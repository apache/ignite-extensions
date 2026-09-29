::    Licensed under the Apache License, Version 2.0 (the "License");
::    you may not use this file except in compliance with the License.
::    You may obtain a copy of the License at
::
::        http://www.apache.org/licenses/LICENSE-2.0
::
::    Unless required by applicable law or agreed to in writing, software
::    distributed under the License is distributed on an "AS IS" BASIS,
::    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::    See the License for the specific language governing permissions and
::    limitations under the License.

::
:: Script that starts BenchmarkServer or BenchmarkDriver.
::

@echo off

set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

if defined CUR_DIR cd %CUR_DIR%

if not defined MAIN_CLASS (
    echo ERROR: Java class is not defined.
    echo Type \"--help\" for usage.
    exit /b
)

if not defined JAVA_HOME (
    echo ERROR: JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 11 or later.
    echo You can also download latest JDK at https://jdk.java.net
    exit /b
)

if not exist "%JAVA_HOME%\bin\java.exe" (
    echo ERROR: JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 11 or later.
    echo You can also download latest JDK at https://jdk.java.net
    exit /b
)

for /f "tokens=3" %%i in ('""%JAVA_HOME%\bin\java.exe" -version 2^>^&1 ^| findstr /i "version""') do set JAVA_VER_STR=%%i
set JAVA_VER_STR=%JAVA_VER_STR:"=%

for /f "delims=.-_+ tokens=1-2" %%v in ("%JAVA_VER_STR%") do (
    if %%v == 1 (set MAJOR_JAVA_VER=%%w) else (set MAJOR_JAVA_VER=%%v)
)

if not defined MAJOR_JAVA_VER set MAJOR_JAVA_VER=0

if %MAJOR_JAVA_VER% LSS 11 (
    echo ERROR: The version of JAVA installed in JAVA_HOME=%JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 11 or later.
    echo You can also download latest JDK at https://jdk.java.net
    exit /b
)

set ARGS=%*

set CP=%CP%;%SCRIPT_DIR%\..\libs\*

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
set JVM_OPTS=-server -Djava.net.preferIPv4Stack=true %JVM_OPTS%

::
:: JDK specific options, required by Ignite for JDK 11 and later (see Ignite bin\include\jvmdefaults.bat).
::
if %MAJOR_JAVA_VER% GEQ 11 if %MAJOR_JAVA_VER% LSS 15 set JVM_OPTS=--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --illegal-access=permit %JVM_OPTS%

if %MAJOR_JAVA_VER% GEQ 15 set JVM_OPTS=--add-opens=java.base/jdk.internal.access=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.sql/java.sql=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.management/sun.management=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED %JVM_OPTS%

::
:: Assertions are disabled by default.
:: If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
::
set ENABLE_ASSERTIONS="0"

::
:: Set '-ea' options if assertions are enabled.
::
if %ENABLE_ASSERTIONS% == "1" set JVM_OPTS=%JVM_OPTS% -ea

"%JAVA_HOME%\bin\java.exe" %JVM_OPTS% -cp %CP% %MAIN_CLASS% %ARGS%
