/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * FSCK tool to recover failed directory mutations guarded by GCS Connector Cooperative Locking
 * feature.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar com.google.cloud.hadoop.fs.gcs.CoopLockFsck \
 *     --{check,rollBack,rollForward} gs://<bucket_name> [all|<operation-id>]
 * }</pre>
 */
public class CoopLockFsck extends Configured implements Tool {

  static final String COMMAND_CHECK = "--check";
  static final String COMMAND_ROLL_BACK = "--rollBack";
  static final String COMMAND_ROLL_FORWARD = "--rollForward";

  static final String ARGUMENT_ALL_OPERATIONS = "all";

  private static final ImmutableSet<String> FSCK_COMMANDS =
      ImmutableSet.of(COMMAND_CHECK, COMMAND_ROLL_FORWARD, COMMAND_ROLL_BACK);

  public static void main(String[] args) throws Exception {
    checkArgument(args.length > 0, "No arguments are specified");

    if (args.length == 1 && "--help".equals(args[0])) {
      System.out.println(
          "FSCK tool to recover failed directory mutations guarded by"
              + " GCS Connector Cooperative Locking feature."
              + "\n\nUsage:"
              + String.format(
                  "\n\thadoop jar /usr/lib/hadoop/lib/gcs-connector.jar %s"
                      + " --{check,rollBack,rollForward} gs://<bucket_name> [all|<operation_id>]",
                  CoopLockFsck.class.getCanonicalName())
              + "\n\nSupported commands:"
              + String.format("\n\t%s - print out operations status in the bucket", COMMAND_CHECK)
              + String.format(
                  "\n\t%s - recover directory operations in the bucket by rolling them forward",
                  COMMAND_ROLL_FORWARD)
              + String.format(
                  "\n\t%s - recover directory operations in the bucket by rolling them back",
                  COMMAND_ROLL_BACK));
      return;
    }

    // Let ToolRunner handle generic command-line options
    int result = ToolRunner.run(new Configuration(), new CoopLockFsck(), args);

    System.exit(result);
  }

  @Override
  public int run(String[] args) throws Exception {
    String command = args[0];
    checkArgument(FSCK_COMMANDS.contains(command), "Unknown %s command, should be %s", command);

    int expectedArgsNumber = COMMAND_CHECK.equals(command) ? 2 : 3;
    checkArgument(
        args.length == expectedArgsNumber,
        "%s arguments should be specified for %s command, but were: %s",
        expectedArgsNumber,
        command,
        Arrays.asList(args));

    String bucket = args[1];
    checkArgument(
        bucket.startsWith(GoogleCloudStorageFileSystem.SCHEME + "://"),
        "bucket parameter should have 'gs://' scheme");

    String operationId = COMMAND_CHECK.equals(command) ? null : args[2];

    return new CoopLockFsckRunner(getConf(), URI.create(bucket), command, operationId).run();
  }
}
