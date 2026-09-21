# explicit contracts can add rules but cannot be silently dropped

    Code
      dr_write_data(lake, data, "orders")
    Condition
      Error in `fn()`:
      ! This asset uses an explicit contract. Supply contract to keep its checks active.

# custom rule closures are re-evaluated unless explicitly versioned

    Code
      dr_write_data(lake, data, "orders", contract, cache = TRUE)
    Condition
      Error in `fn()`:
      ! Supply code_version to cache custom readers or rules, or leave cache unset.

# reopening refuses an accidental backend switch

    Code
      dr_open_lake(root, backend = "ducklake")
    Condition
      Error:
      ! This folder uses a different backend. Reopen without backend or choose a new folder.

# existing unmarked catalogs are not adopted implicitly

    Code
      dr_open_lake(root)
    Condition
      Error:
      ! This folder is not empty and has no dataraft.json. Use its original dr_lake_config() or choose an empty folder.

# arbitrary nonempty folders are left untouched

    Code
      dr_open_lake(root)
    Condition
      Error:
      ! This folder is not empty and has no dataraft.json. Use its original dr_lake_config() or choose an empty folder.

# a blocked first contracted run still requires a contract after reopen

    Code
      dr_write_data(lake, data.frame(id = 1L), "orders")
    Condition
      Error in `fn()`:
      ! This asset uses an explicit contract. Supply contract to keep its checks active.

# a blocked contract upgrade cannot fall back to the automatic schema

    Code
      dr_write_data(lake, data.frame(id = 1L), "orders")
    Condition
      Error in `fn()`:
      ! This asset uses an explicit contract. Supply contract to keep its checks active.

# a data expression needs a deliberate asset name

    Code
      dr_write_data(lake, data.frame(id = 1L))
    Condition
      Error in `dr_write_data()`:
      ! Supply name when writing a data frame expression, for example name = 'orders'.

# contract drafts keep review explicit with optional metadata

    Code
      dr_write_data(lake, data.frame(id = 1L), "orders", contract = draft)
    Condition
      Error in `dr_write_data()`:
      ! Review the contract draft and call dr_contract_confirm() first.

