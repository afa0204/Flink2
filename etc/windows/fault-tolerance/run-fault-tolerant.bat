@echo off
set APPROOT=C:\code\Flink2

rem flink run -c com.dfheinz.flink.stream.fault.ComputerSumFaultTolerant ..\..\..\build\libs\Flink2-all-1.0.jar localhost 9999

flink run -c com.dfheinz.flink.stream.fault.ComputeSumFaultTolerant %APPROOT%\build\libs\Flink2-all-1.0.jar