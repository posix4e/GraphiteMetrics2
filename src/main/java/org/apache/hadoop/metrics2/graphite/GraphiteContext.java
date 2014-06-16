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
import java.net.SocketException;
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
  private static final int DEFAULT_RETRY_SOCKET_INTERVAL = 60000;
  private static final int DEFAULT_SOCKET_CONNECTION_RETRIES = 10;
  private int retrySocketInterval;
  private int socketConnectionRetries;

  /* Configuration attribute names */
  protected static final String SERVER_NAME_PROPERTY = "servers";
  private static final String RETRY_SOCKET_INTERVAL = "retry_socket_interval";
  private static final String SOCKET_CONNECTION_RETRIES = "socket_connection_retries";
  public final Log LOG = LogFactory.getLog(this.getClass());
  Queue<String> metricsQueue = new LinkedBlockingDeque<String>();
  private List<? extends SocketAddress> metricsServers;

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

    } catch(SocketException e) {
      establishConnection();
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  @Override
  public void init(SubsetConfiguration conf) {
    LOG.info("Initializing the GraphiteSink");
    metricsServers = Servers.parse(conf.getString(SERVER_NAME_PROPERTY),
        DEFAULT_PORT);
    for (SocketAddress metricServer: metricsServers){
      LOG.info("Adding metricServer" + metricServer);
    }

    retrySocketInterval = conf.getInt(RETRY_SOCKET_INTERVAL, DEFAULT_RETRY_SOCKET_INTERVAL);
    socketConnectionRetries = conf.getInt(SOCKET_CONNECTION_RETRIES,DEFAULT_SOCKET_CONNECTION_RETRIES);

    socket = new Socket();
    try {
      socket.connect(metricsServers.get(0));
    } catch(IOException e) {
      LOG.error(e);
    }

  }

  // Retries the connection according to retry parameters obtained through conf
  private void establishConnection() {

    if (socket != null && !socket.isClosed()) {
      try {
        socket.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    socket = new Socket();

    int retryCount = 1;
    while (!socket.isConnected() && retryCount <= socketConnectionRetries) {
      try {
        socket.connect(metricsServers.get(0));
      } catch (IOException e) {
        LOG.error(e);
        LOG.info("Retrying in:" + retrySocketInterval/1000 + "s");
        try {
          Thread.sleep(retrySocketInterval);
          socket = new Socket();
        } catch (InterruptedException e1) {
          LOG.error(e1);
        }
      }
      retryCount++;
    }
  }
}
