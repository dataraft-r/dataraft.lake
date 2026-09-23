\name{dr_backfill}
\alias{dr_backfill}
\title{Replace date partitions over a bounded range}
\description{Uses the existing partition replacement quality gate. Each date is a separate committed release.}
\usage{dr_backfill(lake, product, from, to, partition_by, source_for_date, source_name = NULL)}
\arguments{
\item{lake}{Connected writable lake.}
\item{product}{DataRaft product with one named input and no target.}
\item{from}{First ISO date, inclusive.}
\item{to}{Last ISO date, inclusive.}
\item{partition_by}{Date column identifying partitions.}
\item{source_for_date}{Function returning a complete data frame for one Date.}
\item{source_name}{Existing product source name.}
}
\value{A named list of published run results.}
