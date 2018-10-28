/*
 * Copyright [2017] [Fabio Corneti]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.corneti.elasticsearch.plugin.ingest.geocoding;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.corneti.elasticsearch.plugin.ingest.geocoding.IngestGeocodingPlugin.API_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GeocodingProcessorTests extends ESTestCase {

  static final String KNOWN_ADDRESS = "1600 Amphitheatre Parkway, Mountain+View, CA, US";
  static final String MOCK_KEY = "mock_key";

  /**
   * This mocks returns a valid {@link GeocodingOutput} for the known address if the expected mock key
   * was specified in the settings, otherwise null.
   */
  static final class GeocodingServiceMock extends GeocodingService {

    GeocodingServiceMock(Settings settings) {
      super(settings);
    }

    @Override
    GeocodingOutput geocode(String address) {
      if (API_KEY.get(super.settings).equals(MOCK_KEY)) {
        if (address.equals(KNOWN_ADDRESS)) {
          return new GeocodingOutput(1.0, -1.0);
        }
        return null;
      }
      return new GeocodingOutput(new Exception("Invalid API key (mock)."));
    }

  }

  @SuppressWarnings("unchecked")
  public void testKnownAddressWithMissingAPIKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", KNOWN_ADDRESS);

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final GeocodingService service = new GeocodingService(Settings.EMPTY);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), equalTo(KNOWN_ADDRESS));
      assertThat(ingestDocument.hasField("loc.coordinates"), is(false));
      final List<String> errors = ingestDocument.getFieldValue("loc._errors", List.class);
      assertThat(errors.size(), is(1));
      assertThat(errors.get(0), equalTo("Invalid API key."));
    } finally {
      service.stop();
      service.close();
    }
  }

  @SuppressWarnings("unchecked")
  public void testAddressAsMapWithMissingAPIKey() throws Exception {
    final Map<String, String> addressMap = new HashMap<>();
    addressMap.put("road", "to nowhere");

    final Map<String, Object> document = new HashMap<>();
    document.put("address", addressMap);

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final GeocodingService service = new GeocodingService(Settings.EMPTY);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", Map.class, true), equalTo(addressMap));
      assertThat(ingestDocument.hasField("loc.coordinates"), is(false));
      final List<String> errors = ingestDocument.getFieldValue("loc._errors", List.class);
      assertThat(errors.size(), is(1));
      assertThat(errors.get(0),
          equalTo("field [address] of type [java.util.HashMap] cannot be cast to [java.lang.String]"));
    } finally {
      service.stop();
      service.close();
    }
  }

  public void testEmptyAddressWithMissingAPIKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", " ");

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final GeocodingService service = new GeocodingService(Settings.EMPTY);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), equalTo(" "));
      assertThat(ingestDocument.hasField("loc._errors"), is(false));
      assertThat(ingestDocument.getFieldValue("loc.coordinates", Map.class), nullValue());
    } finally {
      service.stop();
      service.close();
    }
  }

  public void testNullAddressWithMissingAPIKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", null);

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final GeocodingService service = new GeocodingService(Settings.EMPTY);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), nullValue());
      assertThat(ingestDocument.hasField("loc._errors"), is(false));
      assertThat(ingestDocument.getFieldValue("loc.coordinates", Map.class), nullValue());
    } finally {
      service.stop();
      service.close();
    }
  }

  public void testNoAddressWithMissingAPIKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final GeocodingService service = new GeocodingService(Settings.EMPTY);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);
      assertThat(ingestDocument.hasField("loc"), is(false));
    } finally {
      service.stop();
      service.close();
    }
  }

  @SuppressWarnings("unchecked")
  public void testKnownAddressUsingMockWithCorrectApiKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", KNOWN_ADDRESS);

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final Settings settings = Settings.builder()
        .put(API_KEY.getKey(), MOCK_KEY)
        .build();
    final GeocodingService service = new GeocodingServiceMock(settings);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), equalTo(KNOWN_ADDRESS));
      assertThat(ingestDocument.hasField("loc._errors"), is(false));
      assertThat(ingestDocument.hasField("loc.coordinates.lat"), is(true));
      assertThat(ingestDocument.hasField("loc.coordinates.lon"), is(true));
      final Map<String, Double> coordinates = ingestDocument.getFieldValue("loc.coordinates", Map.class);
      assertThat(coordinates.size(), is(2));

      final Double lat = ingestDocument.getFieldValue("loc.coordinates.lat", Double.class);
      final Double lon = ingestDocument.getFieldValue("loc.coordinates.lon", Double.class);
      assertThat(lat, equalTo(1.0));
      assertThat(lon, equalTo(-1.0));
    } finally {
      service.stop();
      service.close();
    }
  }

  @SuppressWarnings("unchecked")
  public void testKnownAddressUsingMockWithIncorrectApiKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", KNOWN_ADDRESS);

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final Settings settings = Settings.builder()
        .put(API_KEY.getKey(), "abc")
        .build();
    final GeocodingService service = new GeocodingServiceMock(settings);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), equalTo(KNOWN_ADDRESS));
      assertThat(ingestDocument.hasField("loc.coordinates"), is(false));
      final List<String> errors = ingestDocument.getFieldValue("loc._errors", List.class);
      assertThat(errors.size(), is(1));
      assertThat(errors.get(0), equalTo("Invalid API key (mock)."));
    } finally {
      service.stop();
      service.close();
    }
  }

  public void testUnknownAddressUsingMockWithCorrectApiKey() throws Exception {
    final Map<String, Object> document = new HashMap<>();
    document.put("address", "unknown");

    final IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

    final Settings settings = Settings.builder()
        .put(API_KEY.getKey(), MOCK_KEY)
        .build();
    final GeocodingService service = new GeocodingServiceMock(settings);
    service.start();

    try {
      final GeocodingProcessor processor = new GeocodingProcessor(service, "geocoding", "address", "loc");
      processor.execute(ingestDocument);

      assertThat(ingestDocument.getFieldValue("address", String.class, true), equalTo("unknown"));
      assertThat(ingestDocument.hasField("loc.coordinates"), is(true));
      assertThat(ingestDocument.hasField("loc._errors"), is(false));
      assertThat(ingestDocument.getFieldValue("loc.coordinates", Map.class), nullValue());
    } finally {
      service.stop();
      service.close();
    }
  }

}

