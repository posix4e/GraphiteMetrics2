/**
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

package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.sink.GraphiteSink;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGraphiteMetrics {
  private AbstractMetric makeMetric(String name, Number value) {
    AbstractMetric metric = mock(AbstractMetric.class);
    when(metric.name()).thenReturn(name);
    when(metric.value()).thenReturn(value);
    return metric;
  }

  @Test
  public void testPutMetrics() {
    GraphiteSink sink = new GraphiteSink();
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "all"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host"));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    metrics.add(makeMetric("foo1", 1.25));
    metrics.add(makeMetric("foo2", 2.25));
    MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

    OutputStreamWriter writer = mock(OutputStreamWriter.class);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

    sink.setWriter(writer);
    sink.putMetrics(record);

    try {
      verify(writer).write(argument.capture());
    } catch (IOException e) {
      e.printStackTrace();
    }

    String result = argument.getValue();

    assertEquals(true,
        result.equals("null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n" +
            "null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n") ||
            result.equals("null.all.Context.Context=all.Hostname=host.foo2 2.25 10\n" +
                "null.all.Context.Context=all.Hostname=host.foo1 1.25 10\n"));
  }

  @Test
  public void testPutMetrics2() {
    GraphiteSink sink = new GraphiteSink();
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "all"));
    tags.add(new MetricsTag(MsInfo.Hostname, null));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    metrics.add(makeMetric("foo1", 1));
    metrics.add(makeMetric("foo2", 2));
    MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

    OutputStreamWriter writer = mock(OutputStreamWriter.class);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

    sink.setWriter(writer);
    sink.putMetrics(record);

    try {
      verify(writer).write(argument.capture());
    } catch (IOException e) {
      e.printStackTrace();
    }

    String result = argument.getValue();

    assertEquals(true,
        result.equals("null.all.Context.Context=all.foo1 1 10\n" +
            "null.all.Context.Context=all.foo2 2 10\n") ||
            result.equals("null.all.Context.Context=all.foo2 2 10\n" +
                "null.all.Context.Context=all.foo1 1 10\n"));
  }
}
