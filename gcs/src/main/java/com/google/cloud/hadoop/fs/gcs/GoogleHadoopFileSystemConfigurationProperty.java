package com.google.cloud.hadoop.fs.gcs;

import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;

/** HDFS configuration property */
public class GoogleHadoopFileSystemConfigurationProperty<T> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

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
    Map.Entry<String, String> keyValue = getOrNull(config::get, key, deprecatedKeys);
    return keyValue == null
        ? logProperty(key, defaultValue)
        : logProperty(keyValue.getKey(), getterFn.apply(keyValue.getKey(), defaultValue));
  }

  T get(Function<String, T> getterFn) {
    Map.Entry<String, T> keyValue = getOrNull(getterFn, key, deprecatedKeys);
    return keyValue == null
        ? logProperty(key, defaultValue)
        : logProperty(keyValue.getKey(), keyValue.getValue());
  }

  private static <S> Map.Entry<String, S> getOrNull(
      Function<String, S> getterFn, String key, List<String> deprecatedKeys) {
    S value = getterFn.apply(key);
    if (value != null && isNotEmptyCollection(value)) {
      return new AbstractMap.SimpleEntry<>(key, value);
    }
    for (String deprecatedKey : deprecatedKeys) {
      value = getterFn.apply(deprecatedKey);
      if (value != null && isNotEmptyCollection(value)) {
        logger.atWarning().log(
            "Using deprecated key '%s', use '%s' key instead.", deprecatedKey, key);
        return new AbstractMap.SimpleEntry<>(deprecatedKey, value);
      }
    }
    return null;
  }

  private static <S> S logProperty(String key, S value) {
    logger.atFine().log("%s = %s", key, value);
    return value;
  }

  private static <S> boolean isNotEmptyCollection(S value) {
    return !(value instanceof Collection && ((Collection) value).isEmpty());
  }
}
