package com.google.cloud.hadoop.gcsio;

import com.google.api.ClientProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.google.storage.v1.StorageOuterClass;
import com.google.protobuf.util.Durations;
import io.grpc.alts.ComputeEngineChannelBuilder;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Provides gRPC stubs for accessing the Storage gRPC API. */
public class StorageStubProvider {

  // The maximum number of times to automatically retry gRPC requests.
  private static final double GRPC_MAX_RETRY_ATTEMPTS = 10;

  // The GCS gRPC server.
  private static final String GRPC_TARGET =
      StorageOuterClass.getDescriptor()
          .findServiceByName("Storage")
          .getOptions()
          .getExtension(ClientProto.defaultHost);

  private GoogleCloudStorageReadOptions readOptions;
  private ExecutorService backgroundTasksThreadPool;

  public StorageStubProvider(
      GoogleCloudStorageReadOptions readOptions, ExecutorService backgroundTasksThreadPool) {
    this.readOptions = readOptions;
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
  }

  public StorageBlockingStub buildBlockingStub() {
    return StorageGrpc.newBlockingStub(
        ComputeEngineChannelBuilder.forTarget(readOptions.getGrpcServerAddress())
            .defaultServiceConfig(getGrpcServiceConfig())
            .build());
  }

  public StorageStub buildAsyncStub() {
    return StorageGrpc.newStub(
            GoogleDefaultChannelBuilder.forTarget(GRPC_TARGET)
                .defaultServiceConfig(getGrpcServiceConfig())
                .build())
        .withExecutor(backgroundTasksThreadPool);
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
}
