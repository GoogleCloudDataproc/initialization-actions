/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery.mapred;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * A factory to construct TaskAttemptContext objects from either Hadoop 1 or Hadoop 2 code.
 */
public class ReflectedTaskAttemptContextFactory {

  /**
   * The class name in Hadoop 2 that implements the TaskAttemptContext interface
   */
  public static final String TASK_ATTEMPT_CONTEXT_IMPL_CLASS =
      "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl";

  /**
   * Construct a TaskAttemptContext object from the given configuration and TaskAttemptID.
   */
  public static TaskAttemptContext getContext(
      JobConf configuration, TaskAttemptID taskAttemptID) {
    Class<?> clazz;
    try {
      clazz = configuration.getClassByName(TASK_ATTEMPT_CONTEXT_IMPL_CLASS);
    } catch (ClassNotFoundException cnfe) {
      try {
        clazz = configuration.getClassByName(TaskAttemptContext.class.getName());
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(
            "Failed to find either Hadoop1 or Hadoop2 TaskAttemptContext.", e);
      }
    }

    Constructor<?> constructor;
    try {
      constructor = clazz.getConstructor(Configuration.class, TaskAttemptID.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to find TaskAttemptContext constructor(Configuration, "
                  + "TaskAttemptID) on class %s",
              clazz.getName()));
    }

    try {
      return (TaskAttemptContext) constructor.newInstance(configuration, taskAttemptID);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
