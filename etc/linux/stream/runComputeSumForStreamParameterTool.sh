
UBER_JAR=$HOME/code/Flink2/build/libs/Flink2-all-1.0.jar
APPCLASS=com.dfheinz.flink.stream.basic.ComputeSumForStreamParameterTool

flink run -c $APPCLASS $UBER_JAR --host captain --port 9999

