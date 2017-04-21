# bz-parse-aggregate

Sample example of working with csv with akka-streams.

Environment: **java 8**, **sbt 0.13.x**, **scala 2.11**

To build:
```
sbt clean assembly
```

To run find in folder **{projectPath}/target/scala-2.11/** assembled **jar** file
and execute:
```
java -jar bz-parse-aggregate-assembly-1.0.jar PathToInputFile PathToOutputFile periodInSeconds
```
For example:
```
java -jar bz-parse-aggregate-assembly-1.0.jar /folder/in.csv /folder/out.csv 60
```

Example of input data:
```
"TheRealJJ","77.92.76.250","2015-11-30 23:11:40"
"loginAuthTest","37.48.80.201","2015-11-30 23:11:51"
"ksiriusr","123.108.246.205","2015-11-30 23:11:55"
"Swed3n","83.250.54.3","2015-11-30 23:12:21"
```
Example of output:
```
"208.123.223.116","2015-12-01T06:42:42","2015-12-01T06:44:44","lehu:2015-12-01T06:42:42,yemi:2015-12-01T06:44:44"
"104.156.228.121","2015-12-01T06:44:55","2015-12-01T06:45","darknumbers:2015-12-01T06:44:55,darknumbers:2015-12-01T06:45"
"42.118.58.88","2015-12-01T06:48:38","2015-12-01T06:49:17","Yamaha1:2015-12-01T06:48:38,Sofm:2015-12-01T06:49:17"
"116.98.1.188","2015-12-01T06:47:46","2015-12-01T06:48:16","ashe121:2015-12-01T06:47:46,ashe121:2015-12-01T06:48:16"
```
