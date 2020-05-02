package com.google.cloud.hadoop.fs.gcs;

import com.google.auto.value.AutoValue;
import java.time.Duration;

/** Options for the {@link GoogleHadoopSyncableOutputStream}. */
@AutoValue
public abstract class SyncableOutputStreamOptions {

  public static final SyncableOutputStreamOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_SyncableOutputStreamOptions.Builder()
        .setAppendEnabled(false)
        .setMinSyncInterval(Duration.ZERO)
        .setSyncOnFlushEnabled(false);
  }

  public abstract Builder toBuilder();

  /** See {@link Builder#setAppendEnabled} */
  public abstract boolean isAppendEnabled();

  /** See {@link Builder#setMinSyncInterval} */
  public abstract Duration getMinSyncInterval();

  /** See {@link Builder#setSyncOnFlushEnabled} */
  public abstract boolean isSyncOnFlushEnabled();

  /** Mutable builder for the {@link SyncableOutputStreamOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Whether the syncable output stream will append to existing files. */
    public abstract Builder setAppendEnabled(boolean appendEnabled);

    /** The minimal interval between two consecutive hsync()/hflush() calls. */
    public abstract Builder setMinSyncInterval(Duration minSyncInterval);

    /** Whether to implement flush using the sync implementation. */
    public abstract Builder setSyncOnFlushEnabled(boolean syncOnFlushEnabled);

    public abstract SyncableOutputStreamOptions build();
  }
}
