\name{dr_promote}
\alias{dr_promote}
\alias{dr_deprecate}
\alias{dr_retire}
\title{Transition product state}
\description{Records audited transitions from draft to validated, active, deprecated and retired.}
\usage{dr_promote(lake, product, to, validation = NULL, actor = Sys.info()[["user"]])
dr_deprecate(lake, product, actor = Sys.info()[["user"]])
dr_retire(lake, product, actor = Sys.info()[["user"]])}
\arguments{\item{actor}{See function description.}\item{lake}{See function description.}\item{product}{See function description.}\item{to}{See function description.}\item{validation}{See function description.}}
\value{A DataRaft definition, result or transition.}
