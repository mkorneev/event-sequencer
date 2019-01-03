# bz-parse-aggregate

Akka-streams flow that collects sequences of multiple log-ins from a given IP in a specified time window.

Log-in data is loaded from an input CSV file with columns of **username**, **ip**, **timestamp**.

The tool will first sort the input file by the **timestamp** column.

### Usage

Environment: **java 8**, **sbt 0.13.x**, **scala 2.11**

To build:
```
sbt clean assembly
```

To run find assembled **jar** file in **{projectPath}/target/scala-2.11/**
and run it as:
```
java -jar bz-parse-aggregate-assembly-1.0.jar <input-file> <output-file> [period]
```
For example:
```
java -jar bz-parse-aggregate-assembly-1.0.jar /folder/in.csv /folder/out.csv 60
```

Example input data:
```
"TheRealJJ","77.92.76.250","2015-11-30 23:11:40"
"loginAuthTest","37.48.80.201","2015-11-30 23:11:51"
"ksiriusr","77.92.76.250","2015-11-30 23:11:55"
"Swed3n","37.48.80.201","2015-11-30 23:12:21"
```

Example output:
```
"37.48.80.201","2015-11-30 23:11:51","2015-11-30 23:12:21","loginAuthTest:2015-11-30 23:11:51,Swed3n:2015-11-30 23:12:21"
"77.92.76.250","2015-11-30 23:11:40","2015-11-30 23:11:55","TheRealJJ:2015-11-30 23:11:40,ksiriusr:2015-11-30 23:11:55"
```

### Caveats

New line characters are NOT expected to be part of a username.

The tool will work fine with any printable UTF-8 characters in the username, and even quotes if escaped with an additional quote.

The output will be difficult/impossible to parse correctly if usernames can contain colon (:) or comma (,) characters.

### Performance testing results

Input file with 60M records (example file concatenated 200 times with an increment of the year, 2.8GB) \
was sorted in 6 minutes and processed in another 4 minutes (260K records/s) with the output size of 914MB. \
\[2GB of Java heap, 2.2 GHz Intel Core i7\]