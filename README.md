# Get Data from OPEN Data and Generate KPIs using Spark Scala

The purpose of this project is to create a loader for open data using Apache Spark, and process these data to generate a csv file which will contain the number of visitor per Street

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 
See deployment for notes on how to deploy the project on a live system.

## Run the script

```
Within spark-shell run cmd
./bin/spark-submit  --class <classpath> --master local[n]  /chemin/application-jar


```

## Directories

```
├───data : our data storage directory
│   ├───raw : raw directory where we store raw data from api
│   ├───work : work directory where we store processed data as parque
│   ├───business : business zone where we store result
├───src : scala scripts
│   ├───main : script used to calculate KPIs
│   ├───test : script used to run test (to be populated)

```
##Deployment
to deploy
## Authors

* Ferdinand MONSAN