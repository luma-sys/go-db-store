# Go DB Store

ORM prototype library for SQL and MongoDB.

## Summary

- [Go DB Store](#go-db-store)
  - [Summary](#summary)
  - [Methods](#methods)
  - [Usage](#usage)
  - [License](#license)
  - [Team](#team)

## Methods

The following methods are available for both SQL and MongoDB stores:

| Methods         | Description                                                 |
| --------------- | ----------------------------------------------------------- |
| WithTransaction | Starts a transaction and executes the transaction decorator |
| Has             | Returns true if an entity exists by id                      |
| Count           | Returns the number of entities by filtered query            |
| FindById        | Returns an entity by id                                     |
| FindAll         | Returns a paginated list of entities                        |
| Save            | Creates a new entity                                        |
| SaveMany        | Creates multiple entities                                   |
| Update          | Update updates an existing entity                           |
| UpdateMany      | UpdateMany updates multiple entities                        |
| Upsert          | Upsert creates or updates an entity                         |
| UpsertMany      | Creates or updates multiple entities                        |
| Delete          | Deletes an entity by id                                     |
| DeleteMany      | Deletes many entities by match filter                       |

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

## License

This project is licensed under the MIT License - see the [LICENSE](https://opensource.org/license/mit) file for details.

## Team

[Luma Sistemas](https://github.com/luma-sys)

Copyright 2025 - [Luma Sistemas](https://github.com/luma-sys)
