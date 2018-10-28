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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IngestGeocodingPlugin extends Plugin implements IngestPlugin {

  private final Settings settings;
  private GeocodingService geocodingService;

  public static final Setting<String> API_KEY =
      new Setting<>("ingest.geocoding.api_key", "", (value) -> value, Setting.Property.NodeScope,
          Setting.Property.Filtered);

  public IngestGeocodingPlugin(Settings settings) {
    this.settings = settings;
  }

  @Override
  public List<Setting<?>> getSettings() {
    return Arrays.asList(API_KEY);
  }

  @Override
  public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
    this.geocodingService = new GeocodingService(parameters.env.settings());
    return MapBuilder.<String, Processor.Factory>newMapBuilder()
        .put(GeocodingProcessor.TYPE, new GeocodingProcessor.Factory(this.geocodingService))
        .immutableMap();
  }

  @Override
  public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                             ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                             NamedXContentRegistry xContentRegistry, Environment environment,
                                             NodeEnvironment nodeEnvironment,
                                             NamedWriteableRegistry namedWriteableRegistry) {
    final List<Object> components = new ArrayList<>();
    components.add(this.geocodingService);
    return components;
  }
}
