# Go DB Store

ORM prototype library for SQL and MongoDB.

## Summary

- [Go DB Store](#go-db-store)
  - [Summary](#summary)
  - [Methods](#methods)
  - [Usage](#usage)
  - [Tests coverage](#tests-coverage)
    - [MongoDB tests coverage](#mongodb-tests-coverage)
    - [SQL tests coverage](#sql-tests-coverage)
  - [License](#license)
  - [Team](#team)

## Methods

The following methods are available for both SQL and MongoDB stores:

| Methods         | Description                                                  |
| --------------- | ------------------------------------------------------------ |
| WithTransaction | Starts a transaction and executes the transaction decorator  |
| Has             | Returns true if an entity exists by id                       |
| Count           | Returns the number of entities by filtered query             |
| FindById        | Returns an entity by id                                      |
| FindAll         | Returns a paginated list of entities                         |
| Save            | Creates a new entity                                         |
| SaveMany        | Creates multiple entities                                    |
| Update          | Update updates an existing entity                            |
| UpdateMany      | UpdateMany updates fields in multiple entities using filters |
| Upsert          | Upsert creates or updates an entity                          |
| UpsertMany      | Creates or updates multiple entities                         |
| Delete          | Deletes an entity by id                                      |
| DeleteMany      | Deletes many entities by match filter                        |

## Usage

To use MongoDB store:

```go
package some_package

import (
    "go.mongodb.org/mongo-driver/mongo"
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

## Tests coverage

To execute unit test by Docker access this [documentation](DOCKER_TESTS.md).

### MongoDB tests coverage

| Function               | Tested Scenarios                                                                                                                          |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Save**               | Complete fields, automatic timestamps, empty fields, empty slice, negative values, duplicate ID                                           |
| **SaveMany**           | Multiple docs, timestamps, empty slice, partial failure                                                                                   |
| **SaveManyNotOrdered** | Unordered insertion                                                                                                                       |
| **FindById**           | Existing document, non-existent document, empty ID                                                                                        |
| **FindAll**            | No filter, empty filter, boolean, string, operators ($gt, $gte, $lt, $lte, $in, $nin, $regex, $ne), multiple filters, pagination, sorting |
| **Count**              | All, with filter, operators, zero results                                                                                                 |
| **Has**                | Existing, non-existent document, empty ID                                                                                                 |
| **Update**             | String, numeric, boolean, timestamp, slice, non-existent document                                                                         |
| **UpdateMany**         | Single, multiple, common filter, timestamp, operators, validation errors                                                                  |
| **Upsert**             | New document, update, timestamps, custom filter                                                                                           |
| **UpsertMany**         | Multiple new, updates, mix of operations                                                                                                  |
| **Delete**             | Existing, non-existent, integrity                                                                                                         |
| **DeleteMany**         | Multiple, operators, zero results, nil filter                                                                                             |
| **Edge Cases**         | Special characters, extreme values, empty strings, pagination beyond                                                                      |
| **Performance**        | Batch of 1000, search with filter                                                                                                         |

### SQL tests coverage

| Function               | Tested Scenarios                                                                                                                                                        |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Save**               | Complete fields, automatic ID, minimum fields, empty string, negative values, zero values, large values, no autoincrement, ignored fields                               |
| **SaveMany**           | Multiple records, single record, empty slice                                                                                                                            |
| **SaveManyNotOrdered** | Not implemented (returns error)                                                                                                                                         |
| **FindById**           | Existing record, non-existent record, zero ID                                                                                                                           |
| **FindAll**            | No filter, empty filter, boolean, string, operators (**gt, **gte, **lt, **lte, **like, **not_like, **not, **in, **is_null, **is_not_null), multiple filters, pagination |
| **Count**              | All, with filter, operators, zero results, multiple filters                                                                                                             |
| **Has**                | Existing, non-existent, zero ID, negative ID                                                                                                                            |
| **Update**             | String, numeric, boolean, timestamp, multiple fields, non-existent record                                                                                               |
| **UpdateMany**         | Single, multiple, common filter, timestamp, operators, validation errors, rollback                                                                                      |
| **Upsert**             | New record, update, unsupported driver                                                                                                                                  |
| **UpsertMany**         | Multiple new, empty slice                                                                                                                                               |
| **Delete**             | Existing, non-existent, integrity                                                                                                                                       |
| **DeleteMany**         | Multiple, operators, zero results                                                                                                                                       |
| **WithTransaction**    | Success, rollback, SQL operations                                                                                                                                       |
| **buildWhereClause**   | All operators, key sorting                                                                                                                                              |
| **Edge Cases**         | Special characters, extreme values, unicode, empty table                                                                                                                |
| **Performance**        | Batch of 1000, search with filter, count                                                                                                                                |
| **Type Conversion**    | Type conversion when reading from the database                                                                                                                          |

## License

This project is licensed under the MIT License - see the [LICENSE](https://opensource.org/license/mit) file for details.

## Team

[Luma Sistemas](https://github.com/luma-sys)

Copyright 2025 - [Luma Sistemas](https://github.com/luma-sys)
