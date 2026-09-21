# unsupported local formats are rejected without rewriting configuration

    Code
      dr_lake_config(path = root)
    Condition
      Error in `dr_lake_config()`:
      ! Unsupported local configuration format. Create a new lake with this package version.

# corrupt current layer settings are rejected without rewriting them

    Code
      dr_lake_config(path = root)
    Condition
      Error in `dr_lake_config()`:
      ! Invalid dataraft.json layers. Restore the folder's original configuration.

