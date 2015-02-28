/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * strings for org.apache.hadoop.mapreduce objects
 */
public class HadoopToStringUtil {

  public static String toString(InputSplit input) throws IOException, InterruptedException {
    if (input == null) {
      return "null";
    }

    String result = "InputSplit::";
    result += " length:" + input.getLength();
    result += " locations: " + Arrays.toString(input.getLocations());
    result += " toString(): " + input.toString();
    return result;
  }

  public static String toString(List<InputSplit> input) throws IOException, InterruptedException {
    if (input == null) {
      return "null";
    }

    String result = "List<InputSplit>::";
    result += " size:" + input.size() + " elements: [";
    for (InputSplit is : input) {
      result += toString(is) + ", ";
    }
    result += "]";
    return result;
  }

  public static String toString(TaskAttemptContext input) {
    if (input == null) {
      return "null";
    }

    String result = "TaskAttemptContext::";
    result += " TaskAttemptID:" + input.getTaskAttemptID();
    result += " Status:" + input.getStatus();
    return result;
  }

  public static String toString(JobContext input) {
    if (input == null) {
      return "null";
    }

    String result = "JobContext::";
    result += " JobName:" + input.getJobName();
    result += " Jar:" + input.getJar();
    return result;
  }

}
