/*
 * Copyright 2017-2019 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.redis.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.contrib.redis.common.TracingConfiguration;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.cluster.RedisCluster;
import redis.embedded.util.OS;

public class TracingLettuceClusterTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  private RedisCluster redisServer;

  @Before
  public void before() {
    mockTracer.reset();

    redisServer = new RedisCluster.Builder()
        .withServerBuilder(
            RedisServer.builder().setting("bind 127.0.0.1")
                .redisExecProvider(RedisExecProvider.build()
                    .override(OS.UNIX, "/usr/bin/redis-server")))
        .serverPorts(Arrays.asList(6379, 42000, 42001, 42002, 42003, 42004))
        .numOfReplicates(1)
        .numOfRetries(42)
        .build();
    redisServer.start();
  }

  @After
  public void after() {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void sync_cluster() {
    RedisClusterClient client = RedisClusterClient.create("redis://localhost");

    StatefulRedisClusterConnection<String, String> connection =
        new TracingStatefulRedisClusterConnection<>(client.connect(),
            new TracingConfiguration.Builder(mockTracer).build());
    RedisAdvancedClusterCommands<String, String> commands = connection.sync();

    assertEquals("OK", commands.set("key", "value"));
    assertEquals("value", commands.get("key"));

    connection.close();

    client.shutdown();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void async_cluster() throws Exception {
    RedisClusterClient client = RedisClusterClient.create("redis://localhost");

    StatefulRedisClusterConnection<String, String> connection =
        new TracingStatefulRedisClusterConnection<>(client.connect(),
            new TracingConfiguration.Builder(mockTracer).build());

    RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();

    assertEquals("OK", commands.set("key2", "value2").get(15, TimeUnit.SECONDS));

    assertEquals("value2", commands.get("key2").get(15, TimeUnit.SECONDS));

    connection.close();

    client.shutdown();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

  @Test
  public void async_cluster_continue_span() throws Exception {
    try (Scope ignored = mockTracer.buildSpan("test").startActive(true)) {
      Span activeSpan = mockTracer.activeSpan();

      RedisClusterClient client = RedisClusterClient.create("redis://localhost");

      StatefulRedisClusterConnection<String, String> connection =
          new TracingStatefulRedisClusterConnection<>(client.connect(),
              new TracingConfiguration.Builder(mockTracer).build());

      RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();

      assertEquals("OK",
          commands.set("key2", "value2").toCompletableFuture().thenApply(s -> {
            assertSame(activeSpan, mockTracer.activeSpan());
            return s;
          }).get(15, TimeUnit.SECONDS));

      connection.close();

      client.shutdown();
    }
    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());
  }

}
