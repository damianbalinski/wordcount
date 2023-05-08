### Commands

```bash
# copy file into container
docker cp ./Iowa_Liquor_Sales.csv master:/tmp/Iowa_Liquor_Sales.csv
docker cp ./src/main/resources/percentage.csv master:/tmp/percentage.csv

# create directory in hadoop
docker exec master hadoop fs -mkdir -p /user/hduser/iowa/
docker exec master hadoop fs -mkdir -p /user/hduser/percentage/

# copy file into hadoop
docker exec master hadoop fs -put /tmp/Iowa_Liquor_Sales.csv /user/hduser/iowa/
docker exec master hadoop fs -put /tmp/percentage.csv /user/hduser/percentage/percentage.csv

# copy jar executable into container
docker cp ./target/alcohol-map-reduce-1.0.0.jar master:/tmp/alcohol-map-reduce.jar 
docker cp ./target/percentage-map-reduce-1.0.0.jar master:/tmp/percentage-map-reduce.jar

# run hadoop task
docker exec master hadoop jar /tmp/alcohol-map-reduce.jar /user/hduser/iowa /user/hduser/output/iowa5/
docker exec master hadoop jar /tmp/percentage-map-reduce.jar /user/hduser/iowa /user/hduser/output/iowa4/

# list of files on hadoop
docker exec master hadoop fs -ls /user/hduser/output
docker exec master hadoop fs -ls /user/hduser/output/iowa5/72d47d3b-9836-4aaa-8e5e-30f43b65d8b5/
docker exec master hadoop fs -ls /user/hduser/output/iowa4/3d4a27fe-783d-43fe-9406-b899598e4640/

# file content on hadoop
docker exec master hadoop fs -cat /user/hduser/output/iowa5/72d47d3b-9836-4aaa-8e5e-30f43b65d8b5/part-r-00000

# bash shell from docker container
docker exec -it master bash
```
