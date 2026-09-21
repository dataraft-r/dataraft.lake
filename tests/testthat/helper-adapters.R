local_adapter_method <- function(generic, class, method, env = parent.frame()) {
  table <- get(".__S3MethodsTable__.", envir = asNamespace("dataraft.core"))
  name <- paste(generic, class, sep = ".")
  old <- get0(name, envir = table, inherits = FALSE)
  registerS3method(generic, class, method, envir = asNamespace("dataraft.core"))
  withr::defer(
    {
      if (is.null(old)) {
        rm(list = name, envir = table)
      } else {
        assign(name, old, envir = table)
      }
    },
    envir = env
  )
}
