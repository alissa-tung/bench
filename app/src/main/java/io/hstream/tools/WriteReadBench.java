package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.hstream.*;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class WriteReadBench {

  private static long lastReportTs;
  private static long lastReadSuccessAppends;
  private static long lastReadFailedAppends;

  private static AtomicLong successAppends = new AtomicLong();
  private static AtomicLong failedAppends = new AtomicLong();

  private static long lastFetchedCount = 0;
  private static AtomicLong fetchedCount = new AtomicLong();

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options);

    if (options.helpRequested) {
      CommandLine.usage(options, System.out);
      return;
    }

    HStreamClient client = HStreamClient.builder().serviceUrl(options.serviceUrl).build();

    // Stream
    String streamName = options.streamNamePrefix + UUID.randomUUID();
    client.createStream(streamName, options.streamReplicationFactor, options.streamBacklogDuration);

    // Write
    RateLimiter rateLimiter = RateLimiter.create(options.rateLimit);
    var batchSetting =
        BatchSetting.newBuilder()
            .bytesLimit(options.batchBytesLimit)
            .ageLimit(options.batchAgeLimit)
            .recordCountLimit(-1)
            .build();
    var flowControlSetting =
        FlowControlSetting.newBuilder().bytesLimit(options.totalBytesLimit).build();
    var bufferedProducer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(batchSetting)
            .flowControlSetting(flowControlSetting)
            .build();
    lastReportTs = System.currentTimeMillis();
    lastReadSuccessAppends = 0;
    lastReadFailedAppends = 0;
    new Thread(() -> append(rateLimiter, bufferedProducer, options)).start();

    // Read
    fetch(client, streamName, options);

    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000);
      long now = System.currentTimeMillis();
      long successRead = successAppends.get();
      long failedRead = failedAppends.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (successRead - lastReadSuccessAppends) * 1000 / duration;
      double failurePerSeconds = (double) (failedRead - lastReadFailedAppends) * 1000 / duration;
      double throughput =
          (double) (successRead - lastReadSuccessAppends)
              * options.recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastReadSuccessAppends = successRead;
      lastReadFailedAppends = failedRead;

      long currentFetchedCount = fetchedCount.get();
      double fetchThroughput =
          (double) (currentFetchedCount - lastFetchedCount)
              * options.recordSize
              * 1000
              / duration
              / 1024
              / 1024;
      lastFetchedCount = currentFetchedCount;

      lastReportTs = now;

      System.out.println(
          String.format(
              "[Append]: success %f record/s, failed %f record/s, throughput %f MB/s",
              successPerSeconds, failurePerSeconds, throughput));
      System.out.println(String.format("[Fetch]: throughput %f MB/s", fetchThroughput));
    }
  }

  public static void append(
      RateLimiter rateLimiter, BufferedProducer producer, WriteReadBench.Options options) {
    Random random = new Random();
    Record record = makeRecord(options);
    while (true) {
      rateLimiter.acquire();
      String key = "test_" + random.nextInt(options.orderingKeys);
      record.setOrderingKey(key);
      producer
          .write(record)
          .handle(
              (recordId, throwable) -> {
                if (throwable != null) {
                  failedAppends.incrementAndGet();
                } else {
                  successAppends.incrementAndGet();
                }
                return null;
              });
    }
  }

  public static void fetch(HStreamClient client, String streamName, Options options) {
    var subscriptionId = "bench_WriteRead_sub_" + UUID.randomUUID();
    var subscription =
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(60)
            .build();
    client.createSubscription(subscription);
    for (int i = 0; i < options.consumerCount; i++) {
      Consumer consumer =
          client
              .newConsumer()
              .subscription(subscriptionId)
              .rawRecordReceiver(
                  (receivedRawRecord, responder) -> {
                    fetchedCount.incrementAndGet();
                    responder.ack();
                  })
              .hRecordReceiver(
                  (receivedHRecord, responder) -> {
                    fetchedCount.incrementAndGet();
                    responder.ack();
                  })
              .build();
      consumer.startAsync().awaitRunning();
    }
  }

  static Record makeRecord(Options options) {
    if (options.payloadType.equals("raw")) {
      return makeRawRecord(options);
    }
    return makeHRecord(options);
  }

  static Record makeRawRecord(Options options) {
    Random random = new Random();
    byte[] payload = new byte[options.recordSize];
    random.nextBytes(payload);
    return Record.newBuilder().rawRecord(payload).build();
  }

  static Record makeHRecord(Options options) {
    int paddingSize = options.recordSize > 96 ? options.recordSize - 96 : 0;
    HRecord hRecord =
        HRecord.newBuilder()
            .put("int", 10)
            .put("boolean", true)
            .put("array", HArray.newBuilder().add(1).add(2).add(3).build())
            .put("string", "h".repeat(paddingSize))
            .build();
    return Record.newBuilder().hRecord(hRecord).build();
  }

  static void removeAllStreams(HStreamClient client) {
    var streams = client.listStreams();
    for (var stream : streams) {
      client.deleteStream(stream.getStreamName());
    }
  }

  static class Options {

    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "192.168.0.216:6570";

    @CommandLine.Option(names = "--stream-name-prefix")
    String streamNamePrefix = "write_bench_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(names = "--batch-age-limit", description = "in ms")
    int batchAgeLimit = 10; // ms

    @CommandLine.Option(names = "--batch-bytes-limit", description = "in bytes")
    int batchBytesLimit = 1024 * 1024; // bytes

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.Option(names = "--ordering-keys")
    int orderingKeys = 10;

    @CommandLine.Option(names = "--total-bytes-limit")
    // int totalBytesLimit = batchBytesLimit * orderingKeys * 10;
    int totalBytesLimit = -1;

    @CommandLine.Option(names = "--record-type")
    String payloadType = "raw";

    @CommandLine.Option(names = "--consumer-count")
    int consumerCount = 1;
  }
}
