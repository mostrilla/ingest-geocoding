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

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class GeocodingProcessor extends AbstractProcessor {

  public static final String TYPE = "geocoding";

  private final GeocodingService geocodingService;
  private final String addressField;
  private final String coordinatesField;
  private final String longitudeField;
  private final String latitudeField;
  private final String errorsField;

  public GeocodingProcessor(GeocodingService geocodingService, String tag, String addressField, String targetField) {
    super(tag);
    this.geocodingService = geocodingService;
    this.addressField = addressField;
    this.coordinatesField = targetField + ".coordinates";
    this.latitudeField = this.coordinatesField + ".lat";
    this.longitudeField = this.coordinatesField + ".lon";
    this.errorsField = targetField + "._errors";
  }

  @Override
  public void execute(IngestDocument ingestDocument) {
    if (ingestDocument == null) {
      return;
    }
    if (!ingestDocument.hasField(addressField)) {
      return;
    }

    final String address;
    try {
      address = ingestDocument.getFieldValue(addressField, String.class, true);
    } catch (Throwable t) {
      ingestDocument.setFieldValue(this.errorsField, Collections.singletonList(t.getMessage()));
      return;
    }

    if (address == null || address.trim().equals("")) {
      ingestDocument.setFieldValue(this.coordinatesField, null);
      return;
    }

    final GeocodingOutput output = this.geocodingService.geocode(address);
    if (output == null) {
      ingestDocument.setFieldValue(this.coordinatesField, null);
      return;
    }
    if (output.getErrors().size() > 0) {
      ingestDocument.setFieldValue(this.errorsField, output.getErrors());
    }
    if (output.getLatitude() != null) {
      ingestDocument.setFieldValue(this.latitudeField, output.getLatitude());
    }
    if (output.getLongitude() != null) {
      ingestDocument.setFieldValue(this.longitudeField, output.getLongitude());
    }
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public static final class Factory implements Processor.Factory {

    final GeocodingService geocodingService;

    Factory(GeocodingService geocodingService) {
      this.geocodingService = geocodingService;
    }

    @Override
    public GeocodingProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config) {
      String field = readStringProperty(TYPE, tag, config, "field");
      String targetField = readStringProperty(TYPE, tag, config, "target_field", "location");

      return new GeocodingProcessor(geocodingService, tag, field, targetField);
    }
  }

}
