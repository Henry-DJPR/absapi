# Error handling function
`%iferror%` <- function(a, b) tryCatch({a}, error = function(e){b})


# Check if coercible to date & single length vector
is_scalar_dateish <- function(x){
  correct_length <- length(x) == 1
  coercable <- is.finite(as.Date(x) %iferror% NA)
  return(correct_length & coercable)
}


# Check if coercable to integer & single length vector
is_scalar_integerish <- function(x){
  correct_length <- length(x) == 1
  coercable <- is.finite(as.integer(x) %iferror% NA)
  return(correct_length & coercable)
}


# Check which row a value appears in a data.frame (any column)
which_row <- function(value, table){
  appears_on <- (as.matrix(table) == value) %*% rep(1, ncol(table))
  return(which(as.logical(appears_on)))
}


# ABS date functions
as_abs_date <- function(date, date_code){
  switch(
    date_code,
    A = format.Date(date, "%Y"),
    M = format.Date(date, "%Y-%m"),
    D = format.Date(date, "%Y-%m-%d"),
    B = format.Date(date, "%Y-%m-%d"),
    W = format.Date(date, "%Y-W%V"),
    S = paste0(
      format.Date(date, "%Y-S"),
      ceiling(as.numeric(format.Date(date, "%m")) / 6)
    ),
    Q = paste0(
      format.Date(date, "%Y-Q"),
      ceiling(as.numeric(format.Date(date, "%m")) / 3)
    ),
    N = stop("Minutely data not implimented"),
    H = stop("Hourly data not implimented"),
    stop("Date format not found")
  )
}


parse_abs_date <- function(date, date_code){
  switch(
    date_code,
    A = lubridate::parse_date_time(date, "Y"),
    M = lubridate::parse_date_time(date, "Ym"),
    D = lubridate::parse_date_time(date, "Ymd"),
    B = lubridate::parse_date_time(date, "Ymd"),
    W = lubridate::parse_date_time(paste0(date, "-1"), "YWw"),
    Q = lubridate::parse_date_time(date, "yq"),
    S = lubridate::parse_date_time(substr(date, 1L, 4L), "Y") + months(
      as.integer(substr(date, nchar(date), nchar(date))) * 6 - 6
    ),
    N = stop("Minutely data not implimented"),
    H = stop("Hourly data not implimented"),
    stop("Date format not found")
  )
}



#
