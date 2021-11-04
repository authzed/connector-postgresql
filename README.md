# SpiceDB Postgres Connector - Under Construction

[![GoDoc](https://godoc.org/github.com/authzed/connector-postgresql?status.svg "Go documentation")](https://godoc.org/github.com/authzed/connector-postgresql)
[![Discord Server](https://img.shields.io/discord/844600078504951838?color=7289da&logo=discord "Discord Server")](https://discord.gg/jTysUaxXzM)
[![Twitter](https://img.shields.io/twitter/follow/authzed?color=%23179CF0&logo=twitter&style=flat-square "@authzed on Twitter")](https://twitter.com/authzed)

`connector-postgresql` is a tool that translates data from postgres databases into SpiceDB relationships.

No guarantees are made for the stability of the CLI interface or the format of config files.

See [CONTRIBUTING.md] for instructions on how to contribute and perform common tasks like building the project and running tests.

[CONTRIBUTING.md]: CONTRIBUTING.md

## Getting Started

`connector-postgresql import` will connect to postgres, generate an example schema and config to map postgres data into SpiceDB, and then attempt to sync that data into SpiceDB as relationships.

By default, it will dry-run to show you what would be synced.

It can also be run as two stages: one to generate an example schema and config, and one to actually import relationships based on that config.

### Auto-Import

```sh
$ connector-postgresql import --dry-run=false --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true "postgres://postgres:secret@localhost:5432/mydb?sslmode=disable"
```
- Prints out a zed schema + a config mapping from pg to SpiceDB 
- Appends the generated zed schema to SpiceDB's schema
- Mirrors all relationships into SpiceDB according to that config

### Dry-Run

```sh
$ connector-postgresql import --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true "postgres://postgres:secret@localhost:5432/mydb?sslmode=disable"
```
- Prints out a zed schema + a config mapping from pg to SpiceDB
- Logs relationships that would have been written to SpiceDB

### Custom Config

```sh
$ connector-postgresql import --config=config.yaml --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true "postgres://postgres:secret@localhost:5432/mydb?sslmode=disable"
```
- Uses the provided `config.yaml` to write relationships into SpiceDB
- If the required schema is already in SpiceDB, skip appending it with `--append-schema=false`

#### Example `config.yaml`

```yaml
schema: |2
  definition customer {}

  definition contact {
      relation customer: customer
  }

  definition article {
      relation tags: tags
  }

  definition tags {
      relation article: article
  }
tables:
# for each row in the contacts table
- name: contacts
  relationships:
  # generate a relationship contact:<contact_id>#customer@customer:<customer_id>_<customer_name>
  - resource_type: contact
    resource_id_cols:
    - contact_id
    relation: customer
    subject_type: customer
    subject_id_cols:
    - customer_id
    - customer_name
# for each row in the article_tag table (a join table from articles <-> tags)
- name: article_tag
  relationships:
  # generate a relationship article:<article_id>#tags@tag:<tag_id>
  - resource_type: article
    resource_id_cols:
    - article_id
    relation: tags
    subject_type: tags
    subject_id_cols:
    - tag_id
  # generate a second relationship tags:<tag_id>#article@article:<article_id>
  - resource_type: tags
    resource_id_cols:
    - tag_id
    relation: article
    subject_type: article
    subject_id_cols:
    - article_id
```

## Connect Quickstart

**WARNING**: This is exploratory, and the current implementation has [serious flaws](https://github.com/authzed/connector-postgresql/issues/1) that mean the connector should not be run in production.

The connector can be run continuously with `connector-postgresql run`. 
Running the connector will first run a full import via `connector-postgresql import`, but then it will follow the postgres replication log and sync data as it changes.

```sh
$ connector-postgresql run --dry-run=false --spicedb-endpoint=localhost:50051 --spicedb-token=somesecretkeyhere --spicedb-insecure=true "postgres://postgres:secret@localhost:5432/mydb?sslmode=disable"
```