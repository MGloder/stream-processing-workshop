# Stream-processing-workshop

## Current Status
[Trello Board](https://trello.com/b/Wjp5Rlxt/recommendation-engine)

## Data Source- GDELT
### The GDELT Project
Supported by Google Jigsaw, the GDELT Project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world.
**(Copied from official website)**

## Workflow
![](references/workflow3.png)

## Requirements
### Install and run Pravega
#### [Option 1] from installation package
* [Pravega Reference Quick Start Page](http://pravega.io/docs/latest/getting-started/)
* [Github Download Link: Version 0.4](https://github.com/pravega/pravega/releases/download/v0.4.0/pravega-0.4.0.tgz)

#### [Option 2] from docker
<!-- (We must replace the <ip> with the IP of our machine to connect to Pravega from our local machine. Optionally we can replace latest with the version of Pravega as per the requirement.) -->

 
 `docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone`

### Install and run Flink
#### Run on docker 
* `docker pull flink:scala_2.11`
* `cd references/flink-docker`
* `docker-compose up`

### Install and run Kafka
#### Quick start 
* [Kafka](https://kafka.apache.org/documentation.html#quickstart)

#### Create a new topic
- Example
    - `kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic my-replicated-topic`


#### Start a producer
- Example
    - `kafka-console-producer.sh --broker-list localhost:9092 --topic test`

#### Start a consumer
- Example
    - `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning`

### Run Jobs
#### Option 4

##### Export Data Producer
--class com.machinedoll.projectdemo.jobs.option4.ExportDataProducer

##### Export Data Consumer
--class com.machinedoll.projectdemo.jobs.option4.ExportDataConsumer  
