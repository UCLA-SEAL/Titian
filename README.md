# Titian: Data Provenance for Apache Spark

This branch contains the source code for [Titian: Data Provenance for Apache Spark published at Vldb 2015](https://github.com/maligulzar/bigdebug/blob/titian-2.1/vldb2016-p301-interlandi.pdf)

The journal version of this work is available [here](https://link.springer.com/article/10.1007/s00778-017-0474-5)

The preliminary usage instructions are available at the BigDebug [website](https://sites.google.com/site/sparkbigdebug/)

## Summary of Titian 
Debugging data processing logic in Data-Intensive Scalable Computing (DISC) systems
is a difficult and time consuming effort. Today's DISC systems offer very little tooling
for debugging programs, and as a result programmers spend countless hours collecting
evidence (e.g., from log files) and performing trial and error debugging. To aid this
effort, we built Titian, a library that enables data provenance---tracking data through
transformations---in Apache Spark. Data scientists using the Titian Spark extension
will be able to quickly identify the input data at the root cause of a potential bug
or outlier result. Titian is built directly into the Spark platform and offers data
provenance support at interactive speeds---orders-of-magnitude faster than alternative
solutions---while minimally impacting Spark job performance; observed overheads for
capturing data lineage rarely exceed 30% above the baseline job execution time.

## Team 


This project was done in collaboration with Professor Condie, Kim, and Millstein's group at UCLA. If you encounter any problems, please open an issue or feel free to contact us:

[Matteo Interlandi](https://interesaaat.github.io): was a postdoc at UCLA and now a Senior Scientist at Microsoft; 

[Muhammad Ali Gulzar](https://people.cs.vt.edu/~gulzar/): was a PhD student at UCLA, now an Assistant Professor at Virginia Tech, gulzar@cs.vt.edu;

[Tyson Condie](https://samueli.ucla.edu/people/tyson-condie/): was an Assistant Professor at UCLA, now at Microsoft 

[Miryung Kim](http://web.cs.ucla.edu/~miryung/): Professor at UCLA, miryung@cs.ucla.edu;

## How to cite 
Please refer to our VLDB'16 paper, [Titian: data provenance support in Spark
](http://web.cs.ucla.edu/~miryung/Publications/vldb2016-p301-interlandi.pdf) for more details. 
### Bibtex  
@article{10.14778/2850583.2850595,
author = {Interlandi, Matteo and Shah, Kshitij and Tetali, Sai Deep and Gulzar, Muhammad Ali and Yoo, Seunghyun and Kim, Miryung and Millstein, Todd and Condie, Tyson},
title = {Titian: Data Provenance Support in Spark},
year = {2015},
issue_date = {November 2015},
publisher = {VLDB Endowment},
volume = {9},
number = {3},
issn = {2150-8097},
url = {https://doi.org/10.14778/2850583.2850595},
doi = {10.14778/2850583.2850595},
journal = {Proc. VLDB Endow.},
month = nov,
pages = {216–227},
numpages = {12}
}
[DOI Link](https://doi.org/10.14778/2850583.2850595)

## Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see 
[http://spark.apache.org/developer-tools.html](the Useful Developer Tools page).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark

And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](http://spark.apache.org/developer-tools.html#individual-tests).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](http://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
