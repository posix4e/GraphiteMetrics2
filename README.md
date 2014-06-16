GraphiteContext
===============

Like the GangliaContext for Hadoop, sends metrics to Graphite

Compile:

    $ mvn install

Installation:

In your hadoop-env.sh file (usually in /etc/hadoop/conf/), add the location of the GraphiteContext.jar file into the HADOOP_CLASSPATH

example: export HADOOP_CLASSPATH="/[path_to]/GraphiteMaven-1.1-SNAPSHOT.jar

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

	#Optional parameters for socket connection retry (Values below are default values for the same)
	*.sink.graphite.retry_socket_interval=60000  #in milliseconds
	*.sink.graphite.socket_connection_retries=10  #Set it to 0 if you do not want it to be retried
	
	
