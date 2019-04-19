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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisCluster;

public class TracingLettuceClusterTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  private RedisCluster redisServer;

  @Before
  public void before() {
    mockTracer.reset();

    redisServer = RedisCluster.builder().ephemeral().sentinelCount(3).quorumSize(2)
        .replicationGroup("master1", 1)
        .replicationGroup("master2", 1)
        .replicationGroup("master3", 1)
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
