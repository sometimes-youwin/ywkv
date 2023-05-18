# YWKV

Pronounced Yuu-Kiv. A simple key-value server that uses [redb](https://github.com/cberner/redb) for persistence.

## Usage

* --port: The port to listen on. Defaults to 9958 (YWKV via T9 keyboard).
* --table-name: The name of the `redb` table to use. Defaults to `main`.
* --db-file-name: The name of the `redb` file to read/write on disk. Defaults to `ywkv.redb`.
* token: The bearer auth token to check GET/POST requests against. Required.

```
ywkv [--port value] [--table-name value] [--db-file-name value] <token>
```

### Writing a value

Request:

```bash
curl -X POST -H "Authorization: Bearer hello" localhost:9958/hello -d "world" | jq -C
```

Response (200):

```json
{
  "value": "",
  "status": "SuccessNew"
}
```

### Reading a value

Request:

```bash
curl -X GET -H "Authorization: Bearer hello" localhost:9958/hello | jq -C
```

Response (200):

```json
{
  "value": "world",
  "status": "Found"
}
```

### Reading a value from an empty table

Request:

```bash
curl -X GET -H "Authorization: Bearer hello" localhost:9958/hello | jq -C
```

Response (500):

```json
{
  "value": "Table 'main' does not exist",
  "status": "Failure"
}
```

### Overwriting a value

Request:

```bash
curl -X POST -H "Authorization: Bearer hello" localhost:9958/hello -d "world" | jq -C
```

Response (200):

```json
{
  "value": "world",
  "status": "SuccessOverwrite"
}
```

### Reading a missing value

Request:

```bash
curl -X GET -H "Authorization: Bearer hello" localhost:9958/missing | jq -C
```

Response (404):

```json
{
  "value": "",
  "status": "Missing"
}
```
