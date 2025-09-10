@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem
@rem SPDX-License-Identifier: Apache-2.0
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  zm-scpro-elmis-dispensations-pipeline startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and ZM_SCPRO_ELMIS_DISPENSATIONS_PIPELINE_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\zm-scpro-elmis-dispensations-pipeline-1.0-SNAPSHOT.jar;%APP_HOME%\lib\httpclient-4.5.14.jar;%APP_HOME%\lib\hapi-structures-v25-2.5.1.jar;%APP_HOME%\lib\jackson-annotations-2.19.2.jar;%APP_HOME%\lib\jackson-datatype-jsr310-2.19.2.jar;%APP_HOME%\lib\jackson-databind-2.19.2.jar;%APP_HOME%\lib\jackson-core-2.19.2.jar;%APP_HOME%\lib\log4j-slf4j-impl-2.25.1.jar;%APP_HOME%\lib\log4j-core-2.25.1.jar;%APP_HOME%\lib\log4j-api-2.25.1.jar;%APP_HOME%\lib\flink-clients-1.20.2.jar;%APP_HOME%\lib\flink-optimizer-1.20.2.jar;%APP_HOME%\lib\flink-datastream-1.20.2.jar;%APP_HOME%\lib\flink-streaming-java-1.20.2.jar;%APP_HOME%\lib\flink-runtime-1.20.2.jar;%APP_HOME%\lib\flink-java-1.20.2.jar;%APP_HOME%\lib\httpcore-4.4.16.jar;%APP_HOME%\lib\commons-logging-1.2.jar;%APP_HOME%\lib\commons-codec-1.11.jar;%APP_HOME%\lib\hapi-base-2.5.1.jar;%APP_HOME%\lib\flink-rpc-akka-loader-1.20.2.jar;%APP_HOME%\lib\flink-hadoop-fs-1.20.2.jar;%APP_HOME%\lib\flink-core-1.20.2.jar;%APP_HOME%\lib\flink-datastream-api-1.20.2.jar;%APP_HOME%\lib\flink-core-api-1.20.2.jar;%APP_HOME%\lib\flink-metrics-core-1.20.2.jar;%APP_HOME%\lib\flink-annotations-1.20.2.jar;%APP_HOME%\lib\flink-rpc-core-1.20.2.jar;%APP_HOME%\lib\slf4j-api-1.7.36.jar;%APP_HOME%\lib\commons-cli-1.5.0.jar;%APP_HOME%\lib\jsr305-1.3.9.jar;%APP_HOME%\lib\commons-text-1.10.0.jar;%APP_HOME%\lib\commons-compress-1.26.0.jar;%APP_HOME%\lib\commons-lang3-3.14.0.jar;%APP_HOME%\lib\commons-math3-3.6.1.jar;%APP_HOME%\lib\chill-java-0.7.6.jar;%APP_HOME%\lib\joda-time-2.1.jar;%APP_HOME%\lib\flink-shaded-asm-9-9.5-17.0.jar;%APP_HOME%\lib\flink-shaded-jackson-2.14.2-17.0.jar;%APP_HOME%\lib\snakeyaml-engine-2.6.jar;%APP_HOME%\lib\kryo-2.24.0.jar;%APP_HOME%\lib\commons-collections-3.2.2.jar;%APP_HOME%\lib\flink-queryable-state-client-java-1.20.2.jar;%APP_HOME%\lib\flink-shaded-guava-31.1-jre-17.0.jar;%APP_HOME%\lib\commons-io-2.15.1.jar;%APP_HOME%\lib\flink-shaded-netty-4.1.91.Final-17.0.jar;%APP_HOME%\lib\flink-shaded-zookeeper-3-3.7.1-17.0.jar;%APP_HOME%\lib\javassist-3.24.0-GA.jar;%APP_HOME%\lib\snappy-java-1.1.10.4.jar;%APP_HOME%\lib\async-profiler-2.9.jar;%APP_HOME%\lib\lz4-java-1.8.0.jar;%APP_HOME%\lib\flink-file-sink-common-1.20.2.jar;%APP_HOME%\lib\flink-connector-datagen-1.20.2.jar;%APP_HOME%\lib\minlog-1.2.jar;%APP_HOME%\lib\objenesis-2.1.jar


@rem Execute zm-scpro-elmis-dispensations-pipeline
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %ZM_SCPRO_ELMIS_DISPENSATIONS_PIPELINE_OPTS%  -classpath "%CLASSPATH%" zm.gov.moh.hie.scp.StreamingJob %*

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable ZM_SCPRO_ELMIS_DISPENSATIONS_PIPELINE_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%ZM_SCPRO_ELMIS_DISPENSATIONS_PIPELINE_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
