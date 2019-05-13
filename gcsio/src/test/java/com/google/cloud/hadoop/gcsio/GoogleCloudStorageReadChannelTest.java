/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.createReadChannel;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.dataRangeResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.dataResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.metadataResponse;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.DateTime;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageReadChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageReadChannelTest {

  @Test
  public void metadataInitialization_eager() throws IOException {
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            metadataResponse(
                new StorageObject()
                    .setBucket(BUCKET_NAME)
                    .setName(OBJECT_NAME)
                    .setSize(new BigInteger("123"))
                    .setGeneration(1L)
                    .setMetageneration(1L)
                    .setUpdated(new DateTime(new Date()))));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(true).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    assertThat(requests).hasSize(1);
    assertThat(readChannel.size()).isEqualTo(123);
    assertThat(requests).hasSize(1);
  }

  @Test
  public void metadataInitialization_lazy() throws IOException {
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            metadataResponse(
                new StorageObject()
                    .setBucket(BUCKET_NAME)
                    .setName(OBJECT_NAME)
                    .setSize(new BigInteger("123"))
                    .setGeneration(1L)
                    .setMetageneration(1L)
                    .setUpdated(new DateTime(new Date()))));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    assertThat(requests).isEmpty();
    assertThat(readChannel.size()).isEqualTo(123);
    assertThat(requests).hasSize(1);
  }

  @Test
  public void fadviseAuto_onForwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // 1st read request response
            dataRangeResponse(Arrays.copyOfRange(testData, 1, testData.length), 1, testData.length),
            // 2nd read request response
            dataRangeResponse(testData2, seekPosition, testData2.length));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFastFailOnNotFound(false)
            .setFadvise(Fadvise.AUTO)
            .setMinRangeRequestSize(1)
            .setInplaceSeekLimit(2)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] readBytes = new byte[1];

    readChannel.position(1);
    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[1]});

    readChannel.position(seekPosition);
    assertThat(readChannel.randomAccess).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[seekPosition]});
    assertThat(readChannel.randomAccess).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=1-", "bytes=5-5").inOrder();
  }

  @Test
  public void fadviseAuto_onBackwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // 1st read request response
            dataRangeResponse(testData2, seekPosition, testData2.length),
            // 2nd read request response
            dataRangeResponse(testData, 0, testData.length));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFastFailOnNotFound(false)
            .setFadvise(Fadvise.AUTO)
            .setMinRangeRequestSize(1)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] readBytes = new byte[1];

    readChannel.position(seekPosition);

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[seekPosition]});

    readChannel.position(0);
    assertThat(readChannel.randomAccess).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(1);
    assertThat(readBytes).isEqualTo(new byte[] {testData[0]});
    assertThat(readChannel.randomAccess).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=5-", "bytes=0-0").inOrder();
  }

  @Test
  public void footerPrefetch_reused() throws IOException {
    int footeSize = 2;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    int footerStart = testData.length - footeSize;
    byte[] footer = Arrays.copyOfRange(testData, footerStart, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // Footer prefetch response
            dataRangeResponse(footer, footerStart, testData.length),
            // Footer read miss request response
            dataResponse(new byte[] {testData[footerStart - 1]}));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFastFailOnNotFound(false)
            .setFadvise(Fadvise.RANDOM)
            .setMinRangeRequestSize(footeSize)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    assertThat(requests).isEmpty();

    byte[] readBytes = new byte[2];

    // Force lazy footer prefetch
    readChannel.position(footerStart);
    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(2);
    assertThat(readChannel.size()).isEqualTo(testData.length);
    assertThat(readBytes).isEqualTo(Arrays.copyOfRange(testData, footerStart, testData.length));

    readChannel.position(footerStart - 1);

    assertThat(readChannel.read(ByteBuffer.wrap(readBytes))).isEqualTo(2);
    assertThat(readBytes)
        .isEqualTo(Arrays.copyOfRange(testData, footerStart - 1, testData.length - 1));

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=8-9", "bytes=7-7").inOrder();
  }
}
