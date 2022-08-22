#!/bin/bash

mvn install:install-file -Dfile=spark-sql-perf_2.12.jar -DgroupId=com.databricks -DartifactId=spark-sql-perf_2.12 -Dversion=0.5.1-SNAPSHOT -Dpackaging=jar
