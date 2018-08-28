package com.google.cloud.hadoop.fs.gcs;

import com.google.common.base.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.util.Collection;
import java.util.List;
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

  public T get(Configuration config, BiFunction<String, T, T> getterFn) {
    Pair<String, String> keyValue = getOrNull(config::get, key, deprecatedKeys);
    return keyValue == null
        ? logProperty(key, defaultValue)
        : logProperty(keyValue.first, getterFn.apply(keyValue.first, defaultValue));
  }

  public T get(Function<String, T> getterFn) {
    Pair<String, T> keyValue = getOrNull(getterFn, key, deprecatedKeys);
    return keyValue == null
        ? logProperty(key, defaultValue)
        : logProperty(keyValue.first, keyValue.second);
  }

  private static <S> Pair<String, S> getOrNull(
      Function<String, S> getterFn, String key, List<String> deprecatedKeys) {
    S value = getterFn.apply(key);
    if (value != null && isNotEmptyCollection(value)) {
      return Pair.of(key, value);
    }
    for (String deprecatedKey : deprecatedKeys) {
      value = getterFn.apply(deprecatedKey);
      if (value != null && isNotEmptyCollection(value)) {
        logger.atWarning().log(
            "Using deprecated key '%s', use '%s' key instead.", deprecatedKey, key);
        return Pair.of(deprecatedKey, value);
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
