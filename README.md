# SpiceDB Postgres Connector - Under Construction

**WARNING**: This is exploratory, and the current implementation has serious flaws that mean the connector should not be run in production.

## Quickstart (Example)

Generate a config based on an existing postgres instance:

```sh
$ postgresconnector config --postgres="postgres://postgres:secret@localhost:55172/db25cf8fbe?sslmode=disable" > config.json
```

This will generate a best-effort syncing config based on your database schema:

```json
{
  "article": [],
  "article_tag": [
    {
      "resource_type": "article_tag",
      "subject_type": "article",
      "relation": "fk_article",
      "resource_id_cols": [
        "article_id",
        "tag_id"
      ],
      "subject_id_cols": [
        "article_id"
      ]
    },
    {
      "resource_type": "article_tag",
      "subject_type": "tag",
      "relation": "fk_tag",
      "resource_id_cols": [
        "article_id",
        "tag_id"
      ],
      "subject_id_cols": [
        "tag_id"
      ]
    }
  ],
  "contacts": [
    {
      "resource_type": "contacts",
      "subject_type": "customers",
      "relation": "fk_customer",
      "resource_id_cols": [
        "contact_id"
      ],
      "subject_id_cols": [
        "customer_id",
        "customer_name"
      ]
    }
  ],
  "customers": [],
  "tag": []
}
```

This config is generated based on foreign keys. We'll modify it to make the relationships we want:

```json
{
  "article_tag": [
    {
      "resource_type": "article",
      "subject_type": "tags",
      "relation": "tags",
      "resource_id_cols": [
        "article_id"
      ],
      "subject_id_cols": [
        "tag_id"
      ]
    },
    {
      "resource_type": "tags",
      "subject_type": "article",
      "relation": "article",
      "resource_id_cols": [
        "tag_id"
      ],
      "subject_id_cols": [
        "article_id"
      ]
    }
  ],
  "contacts": [
    {
      "resource_type": "contacts",
      "subject_type": "customers",
      "relation": "customer",
      "resource_id_cols": [
        "contact_id"
      ],
      "subject_id_cols": [
        "customer_id",
        "customer_name"
      ]
    }
  ],
}
```

The above says:
 - Rows of the `article_tag` join table will generate two relationships, one from `article->tag` and one froom `tag->article`. 
 - Rows of the `contacts` table will generate one relationship from the `contacts` definition to the `customers` definition.

Note that we removed unused tables from the config, and we also renamed the generated relationship name for `contacts#customer`.


We need a corresponding zed schema so that these translated relationships can be written to SpiceDB:

```zed
definition contacts {
    relation customer: customers
}

definition customers {}

definition article {
    relation tags: tags
}

definition tags {
    relation article: article
}
```

That schema is written to spicedb:

```sh
zed schema write schema.zed
```

And then we can run the bulk-import from postgres:

```sh
$ postgresconnector import --spicedb-endpoint="localhost:50051" --spicedb-token="somerandomkeyhere" --spicedb-insecure=true --config=config.json --postgres="postgres://postgres:secret@localhost:55193/db33e9318d?sslmode=disable"
10:27AM INF set log level new level=info
10:27AM INF connecting to postgres database=db33e9318d host=localhost user=postgres
10:27AM INF writing relationships table=article_tag
10:27AM INF writing relationships table=contacts
```

You can see more details with `--log-level=trace`, and you can see what relationships would have been written without syncing to spicedb with `--dry-run`:

```sh
$ postgresconnector import --dry-run --config=config.json --postgres="postgres://postgres:secret@localhost:55193/db33e9318d?sslmode=disable"
9:52AM INF set log level new level=info
9:52AM INF connecting to postgres database=db33e9318d host=localhost user=postgres
9:52AM INF OPERATION_TOUCH rel=tag:1#article@article:1
9:52AM INF OPERATION_TOUCH rel=tag:1#article@article:2
9:52AM INF OPERATION_TOUCH rel=tag:2#article@article:1
9:52AM INF OPERATION_TOUCH rel=article:1#tag@tag:1
9:52AM INF OPERATION_TOUCH rel=article:1#tag@tag:2
9:52AM INF OPERATION_TOUCH rel=article:2#tag@tag:1
9:52AM INF OPERATION_TOUCH rel=contacts:1#customer@customers:1_BigCo
9:52AM INF OPERATION_TOUCH rel=contacts:2#customer@customers:1_BigCo
9:52AM INF OPERATION_TOUCH rel=contacts:3#customer@customers:2_SmallFry
9:52AM INF OPERATION_TOUCH rel=contacts:4#customer@customers:2_SmallFry
9:52AM INF OPERATION_TOUCH rel=contacts:5#customer@customers:2_SmallFry
9:52AM INF OPERATION_TOUCH rel=contacts:6#customer@customers:2_SmallFry
```