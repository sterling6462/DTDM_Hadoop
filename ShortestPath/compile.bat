ECHO ON
set arg1=%1
set arg2=%2
set HADOOP_LIB=C:\hadoop\hadoop-2.6.0\share\hadoop
set HADOOP_CLASSPATH=%HADOOP_LIB%\mapreduce\hadoop-mapreduce-client-core-2.6.0.jar;%HADOOP_LIB%\common\hadoop-common-2.6.0.jar;
md classes
dir classes    
del *.jar
javac -classpath %HADOOP_CLASSPATH% -d classes *.java
jar -cvf %arg1%XYZ.jar -C  classes .
REM hadoop fs -rm -r /inputXYZ
REM hadoop fs -mkdir /inputXYZ
REM hadoop fs -put input.txt /inputXYZ
hadoop fs -rm -r /output* && hadoop jar %arg1%XYZ.jar %arg1% /inputXYZ /outputXYZ && hadoop fs -cat /outputXYZ-6/part-00000