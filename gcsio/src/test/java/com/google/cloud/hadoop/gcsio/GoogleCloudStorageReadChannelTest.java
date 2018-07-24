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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.createReadChannel;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.dataRangeResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.dataResponse;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.metadataResponse;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageReadChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageReadChannelTest {

  @Test
  public void fadviseAuto_onForwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // Metadata request response
            metadataResponse(new StorageObject().setSize(BigInteger.valueOf(testData.length))),
            // 1st read request response
            dataResponse(testData),
            // 2nd read request response
            dataResponse(testData2));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.AUTO)
            .setInplaceSeekLimit(seekPosition - 2)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] read = new byte[1];

    assertThat(readChannel.read(ByteBuffer.wrap(read))).isEqualTo(1);
    assertThat(read).isEqualTo(new byte[] {testData[0]});

    readChannel.position(seekPosition);
    assertThat(readChannel.randomAccess).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(read))).isEqualTo(1);
    assertThat(read).isEqualTo(new byte[] {testData[seekPosition]});
    assertThat(readChannel.randomAccess).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly(null, "bytes=0-", "bytes=5-9").inOrder();
  }

  @Test
  public void fadviseAuto_onBackwardRead_switchesToRandom() throws IOException {
    int seekPosition = 5;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    byte[] testData2 = Arrays.copyOfRange(testData, seekPosition, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // Metadata request response
            metadataResponse(new StorageObject().setSize(BigInteger.valueOf(testData.length))),
            // 1st read request response
            dataResponse(testData2),
            // 2nd read request response
            dataResponse(testData));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFadvise(Fadvise.AUTO).build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);

    byte[] read = new byte[1];

    readChannel.position(seekPosition);

    assertThat(readChannel.read(ByteBuffer.wrap(read))).isEqualTo(1);
    assertThat(read).isEqualTo(new byte[] {testData[seekPosition]});

    readChannel.position(0);
    assertThat(readChannel.randomAccess).isFalse();

    assertThat(readChannel.read(ByteBuffer.wrap(read))).isEqualTo(1);
    assertThat(read).isEqualTo(new byte[] {testData[0]});
    assertThat(readChannel.randomAccess).isTrue();

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly(null, "bytes=5-", "bytes=0-9").inOrder();
  }

  @Test
  public void footerPrefetch_reused() throws IOException {
    int footerPrefetchBytes = 2;
    byte[] testData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09};
    int footerPrefetchStart = testData.length - footerPrefetchBytes;
    byte[] footerPrefetch = Arrays.copyOfRange(testData, footerPrefetchStart, testData.length);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockTransport(
            // Footer prefetch response
            dataRangeResponse(footerPrefetch, footerPrefetchStart, testData.length),
            // Footer read miss request response
            dataResponse(new byte[] {testData[footerPrefetchStart - 1]}));

    List<HttpRequest> requests = new ArrayList<>();

    Storage storage = new Storage(transport, JSON_FACTORY, requests::add);

    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setFooterPrefetchSize(footerPrefetchBytes)
            .build();

    GoogleCloudStorageReadChannel readChannel = createReadChannel(storage, options);
    assertThat(readChannel.size()).isEqualTo(testData.length);

    byte[] read = new byte[footerPrefetchBytes + 1];

    readChannel.position(footerPrefetchStart - 1);

    assertThat(readChannel.read(ByteBuffer.wrap(read))).isEqualTo(3);
    assertThat(read)
        .isEqualTo(Arrays.copyOfRange(testData, footerPrefetchStart - 1, testData.length));

    List<String> rangeHeaders =
        requests.stream().map(r -> r.getHeaders().getRange()).collect(toList());

    assertThat(rangeHeaders).containsExactly("bytes=-2", "bytes=7-7").inOrder();
  }
}
