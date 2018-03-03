/**
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import com.google.api.services.storage.Storage.Objects.Insert;
import com.google.common.base.Optional;

/**
 * Conditions on which a object write should be allowed to continue. Corresponds to setting
 * IfGenerationMatch and IfMetaGenerationMatch in API requests.
 */
public class ObjectWriteConditions {

  /**
   * No conditions for completing the write.
   */
  public static final ObjectWriteConditions NONE = new ObjectWriteConditions();

  private final Optional<Long> contentGenerationMatch;
  private final Optional<Long> metaGenerationMatch;

  public ObjectWriteConditions() {
    metaGenerationMatch = Optional.absent();
    contentGenerationMatch = Optional.absent();
  }

  public ObjectWriteConditions(Optional<Long> contentGenerationMatch,
      Optional<Long> metaGenerationMatch) {
    this.contentGenerationMatch = contentGenerationMatch;
    this.metaGenerationMatch = metaGenerationMatch;
  }

  public boolean hasContentGenerationMatch() {
    return contentGenerationMatch.isPresent();
  }

  public boolean hasMetaGenerationMatch() {
    return metaGenerationMatch.isPresent();
  }

  public long getContentGenerationMatch() {
    return contentGenerationMatch.get();
  }

  public long getMetaGenerationMatch() {
    return metaGenerationMatch.get();
  }

  /**
   * Apply the conditions represented by this object to an Insert operation.
   */
  public void apply(Insert objectToInsert) {
    if (hasContentGenerationMatch()) {
      objectToInsert.setIfGenerationMatch(getContentGenerationMatch());
    }

    if (hasMetaGenerationMatch()) {
      objectToInsert.setIfMetagenerationMatch(getMetaGenerationMatch());
    }
  }
}
