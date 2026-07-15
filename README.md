# Go DB Store

[![Tests](https://github.com/luma-sys/go-db-store/actions/workflows/test.yml/badge.svg)](https://github.com/luma-sys/go-db-store/actions/workflows/test.yml)
[![Release](https://github.com/luma-sys/go-db-store/actions/workflows/release.yml/badge.svg)](https://github.com/luma-sys/go-db-store/actions/workflows/release.yml)

ORM prototype library for SQL and MongoDB.

## Summary

- [Go DB Store](#go-db-store)
  - [Summary](#summary)
  - [Methods](#methods)
  - [Usage](#usage)
  - [Index Management](#index-management)
    - [Simple indexes](#simple-indexes)
    - [Mixed-direction compound indexes](#mixed-direction-compound-indexes)
    - [Batch index creation](#batch-index-creation)
  - [Tests coverage](#tests-coverage)
    - [MongoDB tests coverage](#mongodb-tests-coverage)
    - [SQL tests coverage](#sql-tests-coverage)
  - [License](#license)
  - [Team](#team)

## Methods

The following methods are available for both SQL and MongoDB stores:

| Methods               | Description                                                            |
| --------------------- | ---------------------------------------------------------------------- |
| **EnsureIndexesASC**  | Creates an ascending index (single or compound)                        |
| **EnsureIndexesDESC** | Creates a descending index (single or compound)                        |
| **EnsureIndexes**     | Creates an index with per-field direction control (ASC or DESC)        |
| **EnsureManyIndexes** | Creates multiple indexes in a single round trip (batch via CreateMany) |
| **WithTransaction**   | Starts a transaction and executes the transaction decorator            |
| **Has**               | Returns true if an entity exists by id                                 |
| **Count**             | Returns the number of entities by filtered query                       |
| **FindById**          | Returns an entity by id                                                |
| **FindOne**           | Returns one entity by match filter                                     |
| **FindAll**           | Returns a paginated list of entities                                   |
| **Save**              | Creates a new entity                                                   |
| **SaveMany**          | Creates multiple entities                                              |
| **Update**            | Update updates an existing entity                                      |
| **UpdateMany**        | UpdateMany updates fields in multiple entities using filters           |
| **Upsert**            | Upsert creates or updates an entity                                    |
| **UpsertMany**        | Creates or updates multiple entities                                   |
| **Delete**            | Deletes an entity by id                                                |
| **DeleteOne**         | Deletes one entity by match filter                                     |
| **DeleteMany**        | Deletes many entities by match filter                                  |

## Usage

To use MongoDB store:

```go
package some_package

import (
    "go.mongodb.org/mongo-driver/v2/mongo"
    "github.com/luma-sys/go-db-store/store"
    ...
)

func SomeService(ctx context.Context,db *mongo.Client) {
    coll := db.Collection("contact")
    store.NewMongoStore[Contact](coll)

    contact, err := store.FindById(ctx, "HIBHIB2329N4Y")
    ...
}
```

To use SQL database store:

```go
package some_package

import (
    "database/sql"
    "github.com/luma-sys/go-db-store/store"
    ...
)

func SomeService(ctx context.Context, db *sql.DB) {
    store := store.NewSQLStore[Contact](
        db,
        "contact",
        "id",
    )

    contact, err := store.FindById(ctx, 1)
    ...
}
```

## Index Management

Indexes should be created once at store initialization, before the store is used. The caller is responsible for handling errors.

### Simple indexes

`EnsureIndexesASC` creates ascending indexes. `EnsureIndexesDESC` creates descending indexes. Pass multiple fields to create a compound index with uniform direction.

```go
func NewStore(db *mongo.Database) IssueStore {
    coll := db.Collection("issues")
    st := store.NewMongoStore[Issue](coll)

    // compound index — all fields ASC
    if err := st.EnsureIndexesASC("contract.id", "schedule.startDate"); err != nil {
        panic(err)
    }
    // single field indexes
    if err := st.EnsureIndexesASC("spaces.id"); err != nil {
        panic(err)
    }
    if err := st.EnsureIndexesDESC("schedule.startDate"); err != nil {
        panic(err)
    }

    return &issueStore{Store: st, coll: coll}
}
```

### Mixed-direction compound indexes

Use `EnsureIndexes` when fields in a compound index need different directions. Each `IndexField` specifies a key and a direction (`store.ASC` or `store.DESC`).

```go
// { startDate: 1, price: -1 }
if err := st.EnsureIndexes(
    store.IndexField{Key: "startDate", Dir: store.ASC},
    store.IndexField{Key: "price",     Dir: store.DESC},
); err != nil {
    panic(err)
}
```

`EnsureIndexesASC` and `EnsureIndexesDESC` are convenience wrappers over `EnsureIndexes`.

### Batch index creation

Use `EnsureIndexes` to create multiple indexes in a single MongoDB round trip (`CreateMany`). Each argument is a `[]IndexField` representing one index.

```go
if err := st.EnsureManyIndexes(
    []store.IndexField{{Key: "spaces.id", Dir: store.ASC}},
    []store.IndexField{{Key: "schedule.startDate", Dir: store.DESC}},
    []store.IndexField{
        {Key: "contract.id",         Dir: store.ASC},
        {Key: "schedule.startDate",  Dir: store.ASC},
    },
); err != nil {
    panic(err)
}
```

> **Note:** SQL implementation is not yet available. `EnsureIndex*` methods on SQL stores return `errors.New("method not implemented")`.

## Tests coverage

To execute unit test by Docker access this [documentation](DOCKER_TESTS.md).

### MongoDB tests coverage

| Function               | Tested Scenarios                                                                                                                          |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Save**               | Complete fields, automatic timestamps, empty fields, empty slice, negative values, duplicate ID                                           |
| **SaveMany**           | Multiple docs, timestamps, empty slice, partial failure                                                                                   |
| **SaveManyNotOrdered** | Unordered insertion                                                                                                                       |
| **FindById**           | Existing document, non-existent document, empty ID                                                                                        |
| **FindOne**            | Simple filter, boolean, operators ($gt, $gte, $lt, $in, $regex), multiple filters, empty filter, not found, by \_id                       |
| **FindAll**            | No filter, empty filter, boolean, string, operators ($gt, $gte, $lt, $lte, $in, $nin, $regex, $ne), multiple filters, pagination, sorting |
| **Count**              | All, with filter, operators, zero results                                                                                                 |
| **Has**                | Existing, non-existent document, empty ID                                                                                                 |
| **Update**             | String, numeric, boolean, timestamp, slice, non-existent document                                                                         |
| **UpdateMany**         | Single, multiple, common filter, timestamp, operators, validation errors                                                                  |
| **Upsert**             | New document, update, timestamps, custom filter                                                                                           |
| **UpsertMany**         | Multiple new, updates, mix of operations                                                                                                  |
| **Delete**             | Existing, non-existent, integrity                                                                                                         |
| **DeleteOne**          | Simple filter, boolean, operators ($gt, $gte, $lt, $in, $regex), multiple filters, not found, null filter, empty filter, integrity        |
| **DeleteMany**         | Multiple, operators, zero results, nil filter                                                                                             |
| **Edge Cases**         | Special characters, extreme values, empty strings, pagination beyond                                                                      |
| **Performance**        | Batch of 1000, search with filter                                                                                                         |
| **EnsureIndex**        | Single field ASC, compound ASC, single field DESC, default to ASC, multiple indexes, mixed direction, batch (EnsureIndexes)               |

### SQL tests coverage

| Function               | Tested Scenarios                                                                                                                                                                            |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Save**               | Complete fields, automatic ID, minimum fields, empty string, negative values, zero values, large values, no autoincrement, ignored fields                                                   |
| **SaveMany**           | Multiple records, single record, empty slice                                                                                                                                                |
| **SaveManyNotOrdered** | Not implemented (returns error)                                                                                                                                                             |
| **FindById**           | Existing record, non-existent record, zero ID                                                                                                                                               |
| **FindOne**            | Simple filter, boolean, operators (\*\*gt, \*\*gte, \*\*lt, \*\*lte, \*\*like, \*\*in), multiple filters, empty filter, null filter, not found                                              |
| **FindAll**            | No filter, empty filter, boolean, string, operators (\*\*gt, \*\*gte, \*\*lt, \*\*lte, \*\*like, \*\*not_like, \*\*not, \*\*in, \*\*is_null, \*\*is_not_null), multiple filters, pagination |
| **Count**              | All, with filter, operators, zero results, multiple filters                                                                                                                                 |
| **Has**                | Existing, non-existent, zero ID, negative ID                                                                                                                                                |
| **Update**             | String, numeric, boolean, timestamp, multiple fields, non-existent record                                                                                                                   |
| **UpdateMany**         | Single, multiple, common filter, timestamp, operators, validation errors, rollback                                                                                                          |
| **Upsert**             | New record, update, unsupported driver                                                                                                                                                      |
| **UpsertMany**         | Multiple new, empty slice                                                                                                                                                                   |
| **Delete**             | Existing, non-existent, integrity                                                                                                                                                           |
| **DeleteOne**          | Simple filter, boolean, operators (\*\*gt, \*\*gte, \*\*lt, \*\*lte, \*\*like, \*\*in), multiple filters, not found, null filter, empty filter, integrity                                   |
| **DeleteMany**         | Multiple, operators, zero results                                                                                                                                                           |
| **WithTransaction**    | Success, rollback, SQL operations                                                                                                                                                           |
| **buildWhereClause**   | All operators, key sorting                                                                                                                                                                  |
| **Edge Cases**         | Special characters, extreme values, unicode, empty table                                                                                                                                    |
| **Performance**        | Batch of 1000, search with filter, count                                                                                                                                                    |
| **Type Conversion**    | Type conversion when reading from the database                                                                                                                                              |

## License

This project is licensed under the MIT License - see the [LICENSE](https://opensource.org/license/mit) file for details.

## Team

[Luma Sistemas](https://github.com/luma-sys)

Copyright 2025 - [Luma Sistemas](https://github.com/luma-sys)
