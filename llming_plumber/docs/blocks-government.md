# German Government Blocks

> Free, open APIs from German federal agencies (bund.dev). No API keys required.

Full API catalog at [bund.dev](https://bund.dev/apis) and [github.com/bundesAPI](https://github.com/bundesAPI).

---

### autobahn

Real-time data from German highways — roadworks, traffic warnings, and closures from the Autobahn API.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **region** | str | — | Highway identifier (e.g. `A1`, `A3`, `A7`) |
| **include_closures** | bool | `true` | Include road closures |
| **include_warnings** | bool | `true` | Include traffic warnings |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **incidents** | list | Incidents with `type`, `title`, `description`, `location`, `coordinates` |
| **count** | int | Number of incidents |

**Cache TTL:** 600 seconds (10 minutes)

Useful for logistics planning, delivery route optimization, and fleet management dashboards.

---

### nina

Civil protection warnings from the NINA warning system — severe weather, floods, industrial accidents, police alerts.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **location_name** | str | — | City or region name |
| **include_cancelled** | bool | `false` | Include cancelled/expired warnings |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **warnings** | list | Warnings with `headline`, `severity`, `description`, `sent`, `effective`, `expires` |
| **count** | int | Number of active warnings |

**Cache TTL:** 300 seconds (5 minutes)

Critical for operational safety dashboards and automated alerting.

---

### pegel_online

Real-time water level measurements from German rivers and waterways via Pegel-Online.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **station_name** | str | — | Measuring station name (e.g. `Köln`) |
| **station_id** | str | — | Alternative: station UUID |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **level** | float | Current water level in cm |
| **flow_rate** | float | Flow rate in m³/s |
| **last_measurement** | str | Timestamp of last measurement |

**Cache TTL:** 600 seconds (10 minutes)

Essential for shipping logistics, flood monitoring, and environmental tracking.

---

### feiertage

German public holidays by federal state and year.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **year** | int | current year | Year to query |
| **state** | str | — | Federal state code (e.g. `NW`, `BY`, `BE`, `HE`) |

**Output:**

| Field | Type | Description |
|-------|------|-------------|
| **holidays** | list | Holidays with `name`, `date`, `notes` |
| **count** | int | Number of holidays |

**Cache TTL:** 86400 seconds (24 hours)

State codes:
| Code | State |
|------|-------|
| `BW` | Baden-Württemberg |
| `BY` | Bayern |
| `BE` | Berlin |
| `BB` | Brandenburg |
| `HB` | Bremen |
| `HH` | Hamburg |
| `HE` | Hessen |
| `MV` | Mecklenburg-Vorpommern |
| `NI` | Niedersachsen |
| `NW` | Nordrhein-Westfalen |
| `RP` | Rheinland-Pfalz |
| `SL` | Saarland |
| `SN` | Sachsen |
| `ST` | Sachsen-Anhalt |
| `SH` | Schleswig-Holstein |
| `TH` | Thüringen |

Useful for scheduling, business day calculations, and avoiding shipments on holidays.

---

## Common Pipelines

**Safety monitoring dashboard:**
```
[Timer Trigger (hourly)] → [NINA Warnings] → [Filter (severity >= "Severe")] → [Teams Message]
```

**Logistics planning:**
```
[Timer Trigger (daily)] → [Autobahn (A1)] → [Filter (type = "closure")] → [Send Email to drivers]
```

**Water level alerts:**
```
[Timer Trigger (30min)] → [Pegel Online (Köln)] → [Filter (level > 800)] → [SMS Alert]
```
