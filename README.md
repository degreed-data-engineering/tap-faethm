# tap-faethm

A Singer tap for extracting data from the Faethm Workforce API. This tap was created by Degreed and is designed to work with Meltano for data extraction and loading.

## Features

- Extracts data from Faethm's Workforce API
- Supports multiple data streams:
  - Industries
  - Emerging Skills (per industry)
  - Trending Skills (per industry)
  - Declining Skills (per industry)
- Built using the Singer SDK framework
- Includes rate limiting and timeout handling
- Configurable API base URL and authentication

## Installation

### Using Poetry

```bash
# Install poetry if you haven't already
pipx install poetry

# Install package dependencies
poetry install

# Verify installation
poetry run tap-faethm --help
```


## Configuration

The tap requires the following configuration parameters:

```json
{
  "api_base_url": "https://api.workforce.pearson.com/di/v1",
  "api_key": "YOUR_API_KEY",
  "country_code": "US"
}
```

### Configuration Parameters

- `api_base_url` (optional): Base URL for the Faethm API. Defaults to "https://api.workforce.pearson.com/di/v1"
- `api_key` (required): Your Faethm API authentication token
- `country_code` (required): Country code for data filtering

## Usage with Meltano

Add to your `meltano.yml` file:

```yaml
plugins:
  extractors:
  - name: tap-faethm
    namespace: tap_faethm
    executable: ./tap-faethm.sh
    capabilities:
    - state
    - catalog
    - discover
    settings:
    - name: api_base_url
      value: https://api.workforce.pearson.com/di/v1
    - name: api_key
      kind: string
      sensitive: true
    - name: country_code
      value: US
    config:
      api_base_url: $FAETHM_BASE_URL
      api_key: $FAETHM_API_KEY
      country_code: $FAETHM_COUNTRY_CODE
```

### Testing with Meltano

1. Install the tap:
```bash
meltano install extractor tap-faethm
```

2. Test the discovery mode:
```bash
meltano invoke tap-faethm --discover > catalog.json
```

3. Run the tap:
```bash
meltano invoke tap-faethm
```

## Available Streams

### Industries Stream
- Endpoint: `/industries`
- Primary key: `id`
- Schema:
  - `id` (string)
  - `name` (string)

### Emerging Skills Stream
- Endpoint: `/industries/{industry_id}/skills/emerging`
- Primary key: `id`
- Parent stream: Industries
- Schema:
  - `id` (string)
  - `name` (string)
  - `description` (string)
  - `industry_id` (string)

### Trending Skills Stream
- Endpoint: `/industries/{industry_id}/skills/trending`
- Primary key: `id`
- Parent stream: Industries
- Schema:
  - `id` (string)
  - `name` (string)
  - `description` (string)
  - `industry_id` (string)

### Declining Skills Stream
- Endpoint: `/industries/{industry_id}/skills/declining`
- Primary key: `id`
- Parent stream: Industries
- Schema:
  - `id` (string)
  - `name` (string)
  - `description` (string)
  - `industry_id` (string)
