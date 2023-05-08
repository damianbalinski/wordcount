### Commands

```bash
# copy file into container
docker cp ./Iowa_Liquor_Sales.csv master:/tmp/Iowa_Liquor_Sales.csv        

# create directory in hadoop
docker exec master hadoop fs -mkdir -p /user/hduser/iowa/

# copy file into hadoop
docker exec master hadoop fs -put /tmp/Iowa_Liquor_Sales.csv /user/hduser/iowa/
```
