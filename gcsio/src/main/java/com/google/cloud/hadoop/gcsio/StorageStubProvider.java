package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.ClientProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.google.storage.v1.StorageOuterClass;
import com.google.protobuf.util.Durations;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
public class StorageStubProvider {

  // The maximum number of times to automatically retry gRPC requests.
  private static final double GRPC_MAX_RETRY_ATTEMPTS = 10;

  // The maximum number of media channels this provider will hand out. If more channels are
  // requested, the provider will reuse existing ones, favoring the one with the fewest ongoing
  // requests.
  private static final int MEDIA_CHANNEL_MAX_POOL_SIZE = 12;

  // The GCS gRPC server address.
  private static final String DEFAULT_GCS_GRPC_SERVER_ADDRESS =
      StorageOuterClass.getDescriptor()
          .findServiceByName("Storage")
          .getOptions()
          .getExtension(ClientProto.defaultHost);

  private final GoogleCloudStorageReadOptions readOptions;
  private final ExecutorService backgroundTasksThreadPool;
  private final List<ChannelAndRequestCounter> mediaChannelPool;

  // An interceptor that can be added around a gRPC channel which keeps a count of the number
  // of requests that are active at any given moment.
  final class ActiveRequestCounter implements ClientInterceptor {

    // A count of the number of RPCs currently underway for one gRPC channel channel.
    private final AtomicInteger ongoingRequestCount;

    public ActiveRequestCounter() {
      ongoingRequestCount = new AtomicInteger(0);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
      ClientCall<ReqT, RespT> newCall = channel.newCall(methodDescriptor, callOptions);
      final AtomicBoolean countedCancel = new AtomicBoolean(false);

      // A streaming call might be terminated in one of several possible ways:
      // * The call completes normally -> onClose() will be invoked.
      // * The context is cancelled -> CancellationListener.cancelled() will be called.
      // * The call itself is cancelled (doesn't currently happen) -> ClientCall.cancel() called.
      //
      // It's possible more than one of these could happen, so we use countedCancel to make sure we
      // don't double count a decrement.
      Context.current()
          .addListener(
              context -> {
                if (countedCancel.compareAndSet(false, true)) {
                  ongoingRequestCount.decrementAndGet();
                }
              },
              backgroundTasksThreadPool);

      return new SimpleForwardingClientCall(newCall) {
        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
          if (countedCancel.compareAndSet(false, true)) {
            ongoingRequestCount.decrementAndGet();
          }
          super.cancel(message, cause);
        }

        @Override
        public void start(Listener responseListener, Metadata headers) {
          ongoingRequestCount.incrementAndGet();
          this.delegate()
              .start(
                  new SimpleForwardingClientCallListener(responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                      if (countedCancel.compareAndSet(false, true)) {
                        ongoingRequestCount.decrementAndGet();
                      }
                      super.onClose(status, trailers);
                    }
                  },
                  headers);
        }
      };
    }
  }

  class ChannelAndRequestCounter {
    private final ManagedChannel channel;
    private final ActiveRequestCounter counter;

    public ChannelAndRequestCounter(ManagedChannel channel, ActiveRequestCounter counter) {
      this.channel = channel;
      this.counter = counter;
    }

    public int activeRequests() {
      return counter.ongoingRequestCount.get();
    }
  }

  public StorageStubProvider(
      GoogleCloudStorageReadOptions readOptions, ExecutorService backgroundTasksThreadPool) {
    this.readOptions = readOptions;
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
    this.mediaChannelPool = new ArrayList<>();
  }

  private ChannelAndRequestCounter buildManagedChannel() {
    ActiveRequestCounter counter = new ActiveRequestCounter();
    ManagedChannel channel =
        GoogleDefaultChannelBuilder.forTarget(
                isNullOrEmpty(readOptions.getGrpcServerAddress())
                    ? DEFAULT_GCS_GRPC_SERVER_ADDRESS
                    : readOptions.getGrpcServerAddress())
            .defaultServiceConfig(getGrpcServiceConfig())
            .intercept(counter)
            .build();
    return new ChannelAndRequestCounter(channel, counter);
  }

  public synchronized StorageBlockingStub getBlockingStub() {
    ChannelAndRequestCounter channel;
    if (mediaChannelPool.size() < MEDIA_CHANNEL_MAX_POOL_SIZE) {
      channel = buildManagedChannel();
      mediaChannelPool.add(channel);
    } else {
      channel =
          mediaChannelPool.stream()
              .min(Comparator.comparingInt(ChannelAndRequestCounter::activeRequests))
              .get();
    }
    return StorageGrpc.newBlockingStub(channel.channel);
  }

  public synchronized StorageStub getAsyncStub() {
    ChannelAndRequestCounter channel;
    if (mediaChannelPool.size() < MEDIA_CHANNEL_MAX_POOL_SIZE) {
      channel = buildManagedChannel();
      mediaChannelPool.add(channel);
    } else {
      channel =
          mediaChannelPool.stream()
              .min(Comparator.comparingInt(ChannelAndRequestCounter::activeRequests))
              .get();
    }
    return StorageGrpc.newStub(channel.channel).withExecutor(backgroundTasksThreadPool);
  }

  private Map<String, Object> getGrpcServiceConfig() {
    Map<String, Object> name = ImmutableMap.of("service", "google.storage.v1.Storage");

    Map<String, Object> retryPolicy =
        ImmutableMap.<String, Object>builder()
            .put("maxAttempts", GRPC_MAX_RETRY_ATTEMPTS)
            .put(
                "initialBackoff",
                Durations.fromMillis(readOptions.getBackoffInitialIntervalMillis()).toString())
            .put(
                "maxBackoff",
                Durations.fromMillis(readOptions.getBackoffMaxIntervalMillis()).toString())
            .put("backoffMultiplier", readOptions.getBackoffMultiplier())
            .put("retryableStatusCodes", ImmutableList.of("UNAVAILABLE", "RESOURCE_EXHAUSTED"))
            .build();

    Map<String, Object> methodConfig =
        ImmutableMap.of("name", ImmutableList.of(name), "retryPolicy", retryPolicy);

    // When channel pooling is enabled, force the pick_first grpclb strategy.
    // This is necessary to avoid the multiplicative effect of creating channel pool with
    // `poolSize` number of `ManagedChannel`s, each with a `subSetting` number of number of
    // subchannels.
    // See the service config proto definition for more details:
    // https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto#L182
    Map<String, Object> pickFirstStrategy = ImmutableMap.of("pick_first", ImmutableMap.of());

    Map<String, Object> childPolicy =
        ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy));

    Map<String, Object> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);

    return ImmutableMap.of(
        "methodConfig", ImmutableList.of(methodConfig),
        "loadBalancingConfig", ImmutableList.of(grpcLbPolicy));
  }

  public void shutdown() {
    mediaChannelPool.parallelStream().forEach(c -> c.channel.shutdownNow());
  }
}
