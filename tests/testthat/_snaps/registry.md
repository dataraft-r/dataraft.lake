# unsupported registries are rejected without rewriting evidence

    Code
      registry_init(f$lake)
    Condition
      Error:
      ! Unsupported registry version. Open with a compatible DataRaft version; existing history was not modified.

---

    Code
      dr_connect_lake(config)
    Condition
      Error in `dr_connect_lake()`:
      ! Unsupported registry version. Open with a compatible DataRaft version; existing history was not modified.

---

    Code
      dr_connect_lake(config, read_only = TRUE)
    Condition
      Error in `dr_connect_lake()`:
      ! Unsupported registry version. Create a new lake with this package version.

