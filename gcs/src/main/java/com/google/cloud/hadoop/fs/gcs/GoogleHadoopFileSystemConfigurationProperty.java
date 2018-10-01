package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.hadoop.conf.Configuration;

/** GHFS configuration property */
public class GoogleHadoopFileSystemConfigurationProperty<T> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Joiner COMMA_JOINER = Joiner.on(',');
  private static final Splitter COMMA_SPLITTER = Splitter.on(',');

  private final String key;
  private final List<String> deprecatedKeys;
  private final T defaultValue;

  public GoogleHadoopFileSystemConfigurationProperty(String key) {
    this(key, null);
  }

  public GoogleHadoopFileSystemConfigurationProperty(
      String key, T defaultValue, String... deprecatedKeys) {
    this.key = key;
    this.deprecatedKeys =
        deprecatedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(deprecatedKeys);
    this.defaultValue = defaultValue;
  }

  public String getKey() {
    return key;
  }

  public T getDefault() {
    return defaultValue;
  }

  T get(Configuration config, BiFunction<String, T, T> getterFn) {
    String lookupKey = getLookupKey(config, key, deprecatedKeys);
    return logProperty(lookupKey, getterFn.apply(lookupKey, defaultValue));
  }

  Collection<String> getStringCollection(Configuration config) {
    checkState(
        defaultValue == null || defaultValue instanceof Collection, "Not a collection property");
    String lookupKey = getLookupKey(config, key, deprecatedKeys);
    String valueString =
        config.get(
            lookupKey,
            defaultValue == null ? null : COMMA_JOINER.join((Collection<?>) defaultValue));
    List<String> value = COMMA_SPLITTER.splitToList(nullToEmpty(valueString));
    return logProperty(lookupKey, value);
  }

  private static String getLookupKey(
      Configuration config, String key, List<String> deprecatedKeys) {
    if (config.get(key) != null) {
      return key;
    }
    for (String deprecatedKey : deprecatedKeys) {
      if (config.get(deprecatedKey) != null) {
        logger.atWarning().log(
            "Using deprecated key '%s', use '%s' key instead.", deprecatedKey, key);
        return deprecatedKey;
      }
    }
    return key;
  }

  private static <S> S logProperty(String key, S value) {
    logger.atFine().log("%s = %s", key, value);
    return value;
  }
}
