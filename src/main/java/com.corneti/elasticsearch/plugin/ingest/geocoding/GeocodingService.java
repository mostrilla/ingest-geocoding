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

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.GeocodingApiRequest;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.Geometry;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This services wraps the Geocoding API used by geocoding processors.
 */
public class GeocodingService extends AbstractLifecycleComponent {

  private final Logger logger;
  private GeoApiContext geoApiContext;

  GeocodingService(Settings settings) {
    super(settings);
    this.logger = Loggers.getLogger(getClass(), settings);
    SpecialPermission.check();
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      this.geoApiContext = new GeoApiContext.Builder()
          .apiKey(IngestGeocodingPlugin.API_KEY.get(settings))
          .disableRetries()
          .queryRateLimit(10)
          .build();
      return null;
    });
  }

  /**
   * Calls the Geocoding API.
   *
   * @param address - An address.
   * @return The {@link GeocodingOutput} for the specified address; if no matches are found, returns null.
   */
  GeocodingOutput geocode(String address) {
    final GeocodingApiRequest request = GeocodingApi.newRequest(this.geoApiContext).address(address);

    SpecialPermission.check();
    return AccessController.doPrivileged((PrivilegedAction<GeocodingOutput>) () -> {
      try {
        final GeocodingResult[] results = request.await();
        if (results.length == 0) {
          return null;
        }

        final GeocodingResult result = results[0];
        final Geometry geometry = result.geometry;
        if (geometry == null) {
          return null;
        }

        return new GeocodingOutput(result.geometry.location.lat, result.geometry.location.lng);
      } catch (Throwable t) {
        return new GeocodingOutput(t);
      }
    });
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doStop() {
  }

  @Override
  protected void doClose() throws IOException {
    if (this.geoApiContext == null) {
      return;
    }
    try {
      this.geoApiContext.shutdown();
    } catch (Throwable t) {
      this.logger.info("An error occurred while shutting down the Google Maps API context.", t);
    }
  }

}
