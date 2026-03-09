# Weather Blocks

> Current conditions and forecasts from OpenWeatherMap and the German Meteorological Service (DWD).

Both blocks cache results for 10 minutes to avoid excessive API calls.

---

### weather

Fetch current weather conditions from the OpenWeatherMap API.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **location** | str | — | City name (e.g. `Berlin`, `Munich,DE`) or coordinates |
| **units** | select | `metric` | `metric` (Celsius) or `imperial` (Fahrenheit) |
| **api_key** | str (secret) | — | OpenWeatherMap API key (or set `OPENWEATHER_API_KEY` env var) |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **temperature** | float | Current temperature |
| **condition** | str | Weather condition (e.g. `Clear`, `Rain`, `Clouds`) |
| **humidity** | int | Humidity percentage |
| **wind_speed** | float | Wind speed |

**Cache TTL:** 600 seconds (10 minutes)

---

### dwd_weather

Fetch current weather data from the Deutscher Wetterdienst (DWD) — Germany's official meteorological service. Free, no API key required.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **station_id** | str | — | DWD station ID (e.g. `10381` for Berlin-Dahlem) |
| **location_name** | str | — | Alternative: search by location name |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **temperature** | float | Temperature in Celsius |
| **wind_speed** | float | Wind speed in m/s |
| **precipitation** | float | Precipitation in mm |

**Cache TTL:** 600 seconds (10 minutes)

DWD station IDs can be found at the [DWD open data portal](https://opendata.dwd.de/).
