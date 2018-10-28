# Elasticsearch geocoding Ingest Processor - FOR RECREATIVE PURPOSES ONLY

This is just an experiment to play with Elasticsearch ingest processors.

## Setup

Build the plugin by running `gradle clean check`, then install with:

```
bin/elasticsearch-plugin install file:///<source path>/build/distribution/ingest-geocoding-0.0.1-SNAPSHOT.zip
```

## Settings

* `ingest.geocoding.api_key`: your Geocoding API api key.

## Usage

```
PUT _ingest/pipeline/geotest
{
  "processors": [
    {
      "geocoding" : {
        "field" : "address"
      }
    }
  ],
  "description": "Testing geocoding"
}

PUT /organizations
{
  "mappings": {
    "_doc": {
      "properties": {
        "name": {
          "type": "keyword"
        },
        "address": {
          "type": "text"
        },
        "location": {
          "properties": {
            "coordinates": {
              "type": "geo_point"
            }
          }
        }
      }
    }
  }
}

PUT /organizations/_doc/1?pipeline=geotest
{
  "name": "Google",
  "address" : "1600 Amphitheatre Parkway Mountain View, CA 94043 USA"
}

GET /organizations/_doc/1

{
  "_index": "organizations",
  "_type": "_doc",
  "_id": "1",
  "_version": 1,
  "found": true,
  "_source": {
    "address": "1600 Amphitheatre Parkway Mountain View, CA 94043 USA",
    "name": "Google",
    "location": {
      "coordinates": {
        "lon": -122.0855565,
        "lat": 37.4223827
      }
    }
  }
}
```

## Debugging integration tests

To debug integration tests, execute them with:

```
./gradlew clean integTest --debug-jvm
```

Then connect your IDE to port 8000 on your local interface.

