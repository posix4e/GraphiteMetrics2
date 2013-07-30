GraphiteContext
===============

Like the GangliaContext for Hadoop, sends metrics to Graphite

Compile:

    $ mvn install

Installation:

In your hadoop-env.sh file (usually in /etc/hadoop/conf/), add the location of the GraphiteContext.jar file into the HADOOP_CLASSPATH

example: export HADOOP_CLASSPATH="/[path_to]/GraphiteContext.jar"

Configuration:

In your hadoop-metrics2.properties file, add the following for all metrics

*.sink.graphite.class=org.apache.hadoop.metrics2.graphite.GraphiteContext
# default sampling period
*.period=10
namenode.sink.graphite.servers=localhost:2003
datanode.sink.graphite.servers=localhost:2003
nodemanager.sink.graphite.servers=localhost:2003
resourcemanager.sink.graphite.servers=localhost:2003
secondarynamenode.sink.graphite.servers=localhost:2003


metric.path=@Your Path@ can be used to specify the path in Graphite. Defaults to Platform.Hadoop

Restart Daemons:

    $ sudo /etc/init.d/hadoop-tasktracker restart
    $ sudo /etc/init.d/hadoop-tasktracker restart
    $ sudo /etc/init.d/hadoop-tasktracker restart
    $ sudo /etc/init.d/hadoop-tasktracker restart
