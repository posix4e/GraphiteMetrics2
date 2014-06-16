/*
 * GraphiteContext.java
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.graphite;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.util.Servers;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Metrics context for writing metrics to Graphite.<p/>
 * <p/>
 * This class is configured by setting ContextFactory attributes which in turn
 * are usually configured through a properties file.  All the attributes are
 * prefixed by the contextName. For example, the properties file might contain:
 * <pre>
 * </pre>
 */
@SuppressWarnings("unused")
public class GraphiteContext implements MetricsSink {
  Socket socket;

  private static final int DEFAULT_PORT = 2003;


  /* Configuration attribute names */
  protected static final String SERVER_NAME_PROPERTY = "servers";
  public final Log LOG = LogFactory.getLog(this.getClass());
  Queue<String> metricsQueue = new LinkedBlockingDeque<String>();

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    String hostname = null;
    for (MetricsTag tag: metricsRecord.tags()){
      if (tag.name().equals("Hostname")){
        hostname = tag.value();
      } else {
        hostname = "noHostnameListed";
      }
    }
    
    //To avoid domain name hierarchy to interfere with graphite metric hierarchy
    hostname = hostname.replace('.', '-');

    StringBuilder sb = new StringBuilder();
    long tm = System.currentTimeMillis() / 1000;
    // Graphite doesn't handle milliseconds
    for ( AbstractMetric metric : metricsRecord.metrics()) {
      sb.append(hostname);
      sb.append(".");
      sb.append(metric.name());
      sb.append(".")          ;
      sb.append(metric.type().name());

      sb.append(" ");
      sb.append(metric.value());

      sb.append(" ");
      sb.append(tm);
      sb.append("\n");

      metricsQueue.add(sb.toString());
      sb = new StringBuilder();
    }
  }

  @Override
  public void flush() {
    try {
      Writer writer = new OutputStreamWriter(socket.getOutputStream());
      while (metricsQueue.size() > 0){
        writer.write(metricsQueue.remove());
      }
      writer.flush();

    } catch (IOException e) {
      LOG.error(e);
    }
  }

  @Override
  public void init(SubsetConfiguration conf) {
    LOG.info("Initializing the GraphiteSink");
    List<? extends SocketAddress> metricsServers = Servers.parse(conf.getString(SERVER_NAME_PROPERTY),
        DEFAULT_PORT);
    for (SocketAddress metricServer: metricsServers){
      LOG.info("Adding metricServer" + metricServer);
    }

    try {
      socket = new Socket();
      socket.connect(metricsServers.get(0));
    } catch (IOException e) {
      LOG.error(e);
    }

  }
}
