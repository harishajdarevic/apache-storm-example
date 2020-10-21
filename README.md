## Simple apache storm playground

It contains one spout and one bolt. One spout is fetching data from 
redis and sending to a bolt. After success processing in bolt it is returned
back to a spout and inserted into redis "team" list.

## Prerequites
 
1. Run redis instance on port 7777 ( easiest way as a docker container with a following command:
 
```docker run -d -p 7777:6379 redis``` 

 # How to start topology
 
 There are two ways to start this topology in local mode using LocalCluster(). In that case you don't need to have nimbus,
 supervisor and zookeper running separately.
 
 If you want to submit topology using StormSubmitter then you should start:
 all components mentioned above ( zookeeper, nimbus and supervisor ).
 
 It is recommended that you try to submit topology and start storm UI to get insight about topology from various useful information
 that is provided in storm ui.
 
## Local mode

From the project root run following commands:

```#1 mvn clean```

```#2 mvn package```

```#3 storm jar target/mainModule-1.0-SNAPSHOT.jar storm.StormTopology local ```

## Submit topology 

Before packaging your topology you should ensure that you are having nimbus, zookeper, supervisor up and running.

1. Install ZooKeeper
https://www.tutorialspoint.com/apache_storm/apache_storm_installation.htm

2. In above url you will find instructions for storm installation. Anyway I recommend that you use brew for installing storm

```brew install storm```

Configure your storm:

```
 storm.zookeeper.servers:
  - "localhost"
 storm.local.dir: “/path/to/storm/data(any path)”
 nimbus.host: "localhost"
 supervisor.slots.ports:
 - 6700
 - 6701
 - 6702
 - 6703
 ui.port: 4444
```

After storm is installed and ZooKeeper is up and running in a 3 terminal windows run following commands:

```storm supervisor```

```storm nimbus```

```storm ui```

If everything is ok if you visit http://localhost:4444 you should see something like this:

![Alt text](src/assets/storm-ui.png?raw=true "Title")
From the project root run following commands:

```#1 mvn clean```

```#2 mvn package```

```#3 storm jar target/mainModule-1.0-SNAPSHOT.jar storm.StormTopology ```
