# dataraft.lake 0.1.0.9000

* Keep stateless helpers private and prefix shared implementation interfaces with `dr_internal_`. Move component tests into their owning repository; add minimal and downstream CI.

* Classify known connection and writer-lock failures as backend errors while retaining lake classes. Move local configuration regression tests into this package.

* Initial independent DataRaft package.
