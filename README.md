# dataraft.lake

Publish checked releases to DuckDB or DuckLake. Use `dr_open_lake()` and `dr_close_lake()` for resource ownership, and `dr_target_lake()` to bind a workflow destination. DuckDB is required for execution.

This is an independently installable DataRaft component. The `dataraft`
metapackage provides the shared introduction and re-exports the family API.
See `help(package = "dataraft.lake")` for the component reference.

Install the development version:

```r
install.packages("pak")
pak::pak("dataraft-r/dataraft.lake")
```

[Get started with DataRaft](https://github.com/dataraft-r/dataraft).
