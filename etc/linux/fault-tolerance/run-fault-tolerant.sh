@echo off
APPROOT=$HOME/code/Flink2
APPCLASS=com.dfheinz.flink.stream.fault.ComputeSumFaultTolerant

flink run -c $APPCLASS $APPROOT/build/libs/Flink2-all-1.0.jar