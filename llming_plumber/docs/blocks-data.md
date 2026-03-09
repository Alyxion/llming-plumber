# Data Blocks

> File operations, archives, Redis, and MongoDB — for reading, writing, and managing data.

---

## File Operations

### file_read

Read content from a local file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **path** | str | — | Absolute file path |
| **encoding** | str | `utf-8` | Text encoding |
| **as_base64** | bool | `false` | Return content as Base64 (for binary files) |

**Output:** `content` (str), `size_bytes` (int), `path` (str), `encoding` (str)

---

### file_write

Write content to a local file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **path** | str | — | Absolute file path |
| **content** | str | — | Content to write |
| **mode** | select | `write` | `write` (overwrite) or `append` |
| **encoding** | str | `utf-8` | Text encoding |
| **create_dirs** | bool | `true` | Create parent directories if needed |

**Output:** `path` (str), `size_bytes` (int), `created` (bool)

---

### file_list

List files in a directory, optionally matching a glob pattern.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **directory** | str | — | Directory path |
| **pattern** | str | `*` | Glob pattern (e.g. `*.csv`, `**/*.json`) |
| **recursive** | bool | `false` | Search subdirectories |

**Output:** `files` (list of file info dicts), `file_count` (int)

**Fan-out:** Each file becomes an individual parcel for downstream blocks.

---

### file_move

Move or rename a file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **source** | str | — | Source file path |
| **destination** | str | — | Destination file path |
| **overwrite** | bool | `false` | Overwrite if destination exists |

**Output:** `source` (str), `destination` (str), `moved` (bool)

---

### file_delete

Delete a file.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **path** | str | — | File path to delete |

**Output:** `path` (str), `deleted` (bool)

---

### file_collector

Collect multiple file paths into a single list (for processing in a fan-out).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **file_paths** | list | `[]` | List of file paths |

**Output:** `files` (list), `file_count` (int)

**Fan-out:** Each file becomes an individual parcel.

---

## Archives

### zip_create

Create a zip archive from a dict of filename → content mappings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **files** | dict | `{}` | `{filename: content}` mapping |
| **archive_name** | str | `archive.zip` | Name for the archive |
| **compression** | select | `deflated` | `default`, `stored` (no compression), or `deflated` |

**Output:** `content` (str, base64), `archive_size` (int), `file_count` (int)

---

### zip_extract

Extract files from a zip archive.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **archive_content** | str (base64) | — | Base64-encoded zip file |
| **output_dir** | str | — | Directory to extract into |

**Output:** `files` (list), `file_count` (int), `entries` (list)

**Fan-out:** Each extracted file becomes an individual parcel.

---

### zip_list

List the contents of a zip archive without extracting.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **archive_content** | str (base64) | — | Base64-encoded zip file |

**Output:** `entries` (list of entry info), `entry_count` (int)

**Fan-out:** Each entry becomes an individual parcel.

---

## Redis

All Redis blocks accept an optional `server` field for the Redis connection URL (defaults to the system Redis).

### Basic Key-Value

#### redis_get

Get a value from Redis by key.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Redis key |

**Output:** `key` (str), `value` (str), `exists` (bool)

---

#### redis_set

Set a value in Redis.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Redis key |
| **value** | str | — | Value to store |
| **ttl_seconds** | int | `0` | Time-to-live (0 = no expiry) |

**Output:** `key` (str), `set` (bool)

---

#### redis_delete

Delete a key from Redis.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Redis key to delete |

**Output:** `key` (str), `deleted` (bool)

---

#### redis_keys

Find keys matching a pattern.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **pattern** | str | `*` | Key pattern (supports `*` and `?` wildcards) |

**Output:** `keys` (list), `key_count` (int)

---

#### redis_incr

Atomically increment an integer value.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Redis key |
| **increment** | int | `1` | Increment amount |

**Output:** `key` (str), `value` (int, new value after increment)

---

### Lists

#### redis_list_push

Push a value onto a Redis list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | List key |
| **value** | str | — | Value to push |
| **side** | select | `right` | `left` or `right` |

**Output:** `key` (str), `list_length` (int)

---

#### redis_list_pop

Pop a value from a Redis list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | List key |
| **side** | select | `left` | `left` or `right` |

**Output:** `key` (str), `value` (str), `list_length` (int)

---

#### redis_list_range

Get a range of values from a Redis list.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | List key |
| **start** | int | `0` | Start index |
| **stop** | int | `-1` | Stop index (-1 = end) |

**Output:** `key` (str), `values` (list), `count` (int)

---

### Hashes

#### redis_hash_get

Get a field from a Redis hash.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Hash key |
| **field** | str | — | Field within the hash |

**Output:** `key` (str), `field` (str), `value` (str), `exists` (bool)

---

#### redis_hash_set

Set a field in a Redis hash.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Hash key |
| **field** | str | — | Field within the hash |
| **value** | str | — | Value to set |

**Output:** `key` (str), `field` (str), `set` (bool)

---

### Pub/Sub

#### redis_publish

Publish a message to a Redis channel.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **channel** | str | — | Channel name |
| **message** | str | — | Message to publish |

**Output:** `channel` (str), `subscribers` (int)

---

#### redis_subscribe

Subscribe to a Redis channel and receive messages.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **channel** | str | — | Channel name |
| **timeout_seconds** | int | `10` | How long to listen |

**Output:** `messages` (list), `message_count` (int)

**Fan-out:** Each message becomes an individual parcel.

---

### File Storage

#### redis_file_store

Store a binary file in Redis as Base64 with optional TTL.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Storage key |
| **content** | str (base64) | — | Base64-encoded file content |
| **ttl_seconds** | int | `0` | Time-to-live (0 = no expiry) |

**Output:** `key` (str), `size_bytes` (int)

---

#### redis_file_load

Load a binary file from Redis.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **key** | str | — | Storage key |

**Output:** `key` (str), `content` (str, base64), `size_bytes` (int), `exists` (bool)

---

## MongoDB

All MongoDB blocks connect to the system MongoDB instance by default.

### mongo_find

Query documents from a MongoDB collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **query** | dict | `{}` | MongoDB query filter |
| **projection** | dict | `{}` | Fields to include/exclude |
| **limit** | int | `100` | Maximum documents to return |
| **sort_by** | dict | `{}` | Sort specification, e.g. `{"created_at": -1}` |

**Output:** `documents` (list), `document_count` (int)

**Fan-out:** Each document becomes an individual parcel.

---

### mongo_find_one

Find a single document.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **query** | dict | `{}` | Query filter |
| **projection** | dict | `{}` | Fields to include/exclude |

**Output:** `document` (dict), `found` (bool)

---

### mongo_insert

Insert one or more documents.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **documents** | list | `[]` | Documents to insert |

**Output:** `inserted_ids` (list), `inserted_count` (int)

---

### mongo_update

Update documents matching a query.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **query** | dict | `{}` | Query filter |
| **update** | dict | `{}` | Update operations (e.g. `{"$set": {"status": "done"}}`) |
| **upsert** | bool | `false` | Insert if no match found |

**Output:** `matched_count` (int), `modified_count` (int)

---

### mongo_delete

Delete documents matching a query.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **query** | dict | `{}` | Query filter |

**Output:** `deleted_count` (int)

---

### mongo_aggregate

Run a MongoDB aggregation pipeline.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **pipeline** | list[dict] | `[]` | Aggregation pipeline stages |

**Output:** `documents` (list), `document_count` (int)

**Fan-out:** Each result document becomes an individual parcel.

---

### mongo_count

Count documents matching a query.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **query** | dict | `{}` | Query filter |

**Output:** `count` (int)

---

### mongo_watch

Watch a MongoDB collection for real-time changes (insert, update, delete).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **database** | str | — | Database name |
| **collection** | str | — | Collection name |
| **pipeline** | list | `[]` | Optional aggregation pipeline to filter change events |
| **timeout_seconds** | int | `30` | How long to watch |

**Output:** `events` (list of change events), `event_count` (int)

**Fan-out:** Each change event becomes an individual parcel.
