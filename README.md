# dataraft.lake

**Keep checked data releases and their evidence in a lake.**

Use this package when you need to publish a version, inspect history or reopen an earlier result. It adds storage and release management to products defined in `dataraft.core`. Local DuckDB and optional DuckLake configurations have different requirements; the example uses DuckDB.

[`dataraft` overview](https://github.com/dataraft-r/dataraft) · [Lake reference](https://dataraft-r.github.io/dataraft/packages/dataraft.lake/)

## Try it

Requires the optional `duckdb` package.

```r
library(dataraft.lake)

lake <- dr_open_lake(file.path(tempdir(), "orders-lake"))
orders <- dataraft.core::dr_product(
  "orders", data.frame(id = 1L, amount = 25)
) |>
  dataraft.core::dr_add_contract(c(id = "integer", amount = "numeric")) |>
  dataraft.core::dr_add_quality(~ amount >= 0) |>
  dataraft.core::dr_set_target(dr_target_lake(lake))

result <- dataraft.core::dr_run(orders)
result$status
dr_close_lake(lake)
```

The product is checked before its lake target is published. Close the lake when finished. For a first workflow without DuckDB, start with [core](https://github.com/dataraft-r/dataraft.core) or the [RDS adapter](https://github.com/dataraft-r/dataraft.adapters#try-it).

Install the development package with `pak::pak("dataraft-r/dataraft.lake")`. Read the [lake documentation](https://dataraft-r.github.io/dataraft/packages/dataraft.lake/) before using shared writers or DuckLake storage.
