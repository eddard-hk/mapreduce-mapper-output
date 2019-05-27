mapreduce-mapper-output
=======================

## 개요
<https://eddard.tistory.com/1>

위 블로그에 "Mapper Ouput 비교 WritableComparable vs ORC vs ProtocolBuffer" 제목으로 게시된 글의 소스코드 입니다.

## Build
```bash
mvn clean package
``` 

## Run
```bash
#WritableComparable
hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main comparable INPUT경로 OUPUT경로

#ORC
hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main orc INPUT경로 OUPUT경로

#ProtocolBuffer
hadoop jar mapreduce-mapper-output-1.0-SNAPSHOT.jar org.eddard.mapreduce.Main protobuf INPUT경로 OUPUT경로
```
