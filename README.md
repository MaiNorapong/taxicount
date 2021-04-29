# taxicount

Part of assignment #1 (course: Big Data Platform Analytics).
Hadoop MapReduce program to count taxis ([NYC taxi dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)).

## Requirements

To compile `taxicount.jar` yourself you need [Apache Commons CLI version 1.2](https://archive.apache.org/dist/commons/cli/source/) (the same version Hadoop uses) (aside from other dependencies listed in `pom.xml`).

I included a compiled JAR for ease.

## Commands

1\) & 4) locations with the most number of pickups made by yellow taxis

```sh
hadoop jar taxicount.jar TaxiCountLoc -i /input-folder -o /tmp-folder -r . 1
hadoop jar taxicount.jar TaxiCountLoc -i /tmp-folder -o /output-folder -r . 2
```

First command counts. Second command sorts.  
`TaxiCountLoc.PickupLocMapper.precision` sets the round-to digits for coords.

2\) number of trips made by yellow taxis by days of the week

```sh
hadoop jar taxicount.jar TaxiCountDays -i /input-folder -o /output-folder -r . 1
```

Outputs all days of week.

3\) average distance of trips made by yellow taxi

```sh
hadoop jar taxicount.jar TaxiAvgDist -i /input-folder -o /output-folder -r . 1
```

Outputs sum of distance and count of records. Avg = sum / count.

5\) average duration of trips made by yellow taxi

```sh
hadoop jar taxicount.jar TaxiAcgDuration -i /input-folder -o /output-folder -r . 1
```

Outputs sum of trip duration and count of records. Avg = sum / count.
