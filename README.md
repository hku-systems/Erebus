# GUPA

GUPA is a big-data system that automatically infers a local sensitivity value for enforcing Group Differential Privacy. 
Below shows a simple example demonstrating the functionalities of GUPA.

## Core dependencies

`sudo apt-get insall openjdk-8-jdk maven`

## How to build GUPA

GUPA is built in the same way as Apache Spark i.e., by running:

`build/mvn -DskipTests -T 40 package`

## Running an example

1.Generate a sample dataset:

`mkdir $HOME/test; python gen_data.py --wq ml --path $HOME/test/ml.txt --s 1`

This will create a sample dataset of 100000 records under `$HOME/test/dataset.txt`.

2.Partition the dataset into k partitions:

`python indexing.py --wq index --k 200 --path $HOME/test/ml.txt`

This will partition the dataset (`$HOME/test/ml.txt`) into 200 partitions, 
the partitioned dataset is located in `$HOME/test/ml.txt.gupa`.

3.Running an example: 

`./bin/spark-submit --class edu.hku.dp.e2e.SparkHdfsLRDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/test/ml.txt.gupa 1 9 1000`

## Run GUPA in cluster mode

First start a master by running the following command on a master computer:

`./sbin/start-master.sh -h <ip address of master> -p <port to be used>`

Then start workers by running the following command on a worker computer:

`./sbin/start-slave.sh spark://<ip address of master>:<port to be used>`

Then running `./demo_attack.sh` on the master computer. Note that the input dataset has to be replicated on both master and workers. After finishing testing, stop the master and workers by running `./sbin/stop-master.sh` and `./sbin/stop-slave.sh` on master and worker computers respectively, to release their network resources.


