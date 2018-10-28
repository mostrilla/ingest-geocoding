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

import java.util.Collections;
import java.util.List;

/**
 * Simplified output of a Geocoding API request.
 */
class GeocodingOutput {

  private Double latitude;
  private Double longitude;
  private List<String> errors;

  GeocodingOutput(Double latitude, Double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
    this.errors = Collections.emptyList();
  }

  GeocodingOutput(Throwable t) {
    this.errors = Collections.singletonList(t.getMessage());
  }

  Double getLatitude() {
    return latitude;
  }

  Double getLongitude() {
    return longitude;
  }

  List<String> getErrors() {
    return errors;
  }

}
