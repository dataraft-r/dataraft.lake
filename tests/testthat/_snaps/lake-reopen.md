# both folder entry points retain named roles and read-only opens do not write

    Code
      dr_open_lake(root, layers = c("raw", "products"))
    Condition
      Error:
      ! This folder has different saved layers. Omit layers to reuse its configuration, or choose a new folder.

---

    Code
      dr_setup_lake(path = root, landing = "elsewhere")
    Condition
      Error in `dr_setup_lake()`:
      ! Supply path or explicit catalog, storage and landing settings, not both.

---

    Code
      dr_open_lake(root, backend = "ducklake")
    Condition
      Error:
      ! This folder uses a different backend. Reopen without backend or choose a new folder.

# saved configuration cannot be bypassed by an older definition

    Code
      dr_connect_lake(stale)
    Condition
      Error in `dr_connect_lake()`:
      ! This folder has different saved layers. Omit layers to reuse its configuration, or choose a new folder.
