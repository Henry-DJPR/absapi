

#' ABS API interface
#'
#' @param dataset Single length character string containing dataset name
#' @param max_date Single length date for starting request date
#' @param min_date Single length date for end request date
#' @param ... Optional vectors of required parameter values (e.g.
#' `"State of Departure" = c("New South Wales", "Victoria")`)
#' @param max_query_size Single length integer of maximum number of observations
#' any one API call can request. Automatically splits requests into multiple
#' calls.
#' @param cl A cluster object created by `parallel::makeCluster()`, or an
#' integer to indicate number of child-processes (integer values are ignored on
#' Windows) for parallel evaluations
#' @param api_key Unused
#' @param detail The desired amount of detail to be returned as per API documentation
#' @param scaled_units Should observations be returned as reported units
#' (e.g. '000) or actual values
#' @param method Method to be used for downloading files. Current download
#' methods are "internal", "wininet" (Windows only) "libcurl", "wget" and
#' "curl", and there is a value "auto": see ‘Details’ and ‘Note’ in
#' `download.file()`. The method  can also be set through the option
#'  "download.file.method": see `options()`.
#' @param mode character. The mode with which to write the file. Useful values
#' are "w", "wb" (binary), "a" (append) and "ab". Not used for methods
#' "wget" and "curl".
#'
#' @return a data frame of requested data with both dimension codes and names.
#' @export
#'
#' @examples
#' df <- read_abs_api(
#'   "MERCH_EXP",
#'   "Merchandise Country" = "India",
#'   CL_MERCH_SITC = c("01", "05")
#'   )
#'
read_abs_api <- function(
  dataset,
  max_date = Sys.Date(),
  min_date = max_date - 180,
  ...,
  max_query_size = 10000L,
  cl = NULL,
  api_key = NULL,
  detail = "full",
  scaled_units = FALSE,
  method = "libcurl",
  mode = "w"
){

  # Get objects from options
  abs_api_url <- getOption("abs_api_url", "https://api.data.abs.gov.au")


  # Match arguments
  detail <- match.arg(detail, c("full", "dataonly", "serieskeysonly", "nodata"))


  # Convert other arguments to filter list
  filters <- list(...)


  # Input checks
  if(length(abs_api_url) != 1 | !("character" %in% class(abs_api_url))) stop(
    "abs_api_url option must be a single length character"
  )

  if(!(is_scalar_dateish(max_date) & is_scalar_dateish(min_date))) stop(
    "Dates must be coercible to Date class"
  )

  if(length(filters) != 0 & is.null(names(filters))) stop(
    "All filters must be named"
  )

  if(length(filters) != 0 & any(names(filters) == "")){
    warning("All filters must be named, unnamed filters are unused")
    filters <- filters[names(filters) != ""]
  }

  if(length(filters) != 0 & !all(sapply(filters, is.vector))) stop(
    "All filters must be vectors of required values"
  )

  if(Sys.Date() - lubridate::as_date(min_date) < 90) warning(
    "The ABS server may not have data less than 3 months from the current date"
  )

  if(!is.null(api_key)) warning(
    "API keys are not yet implimented"
  )

  if(!("logical" %in% class(scaled_units))) stop(
    "scaled_units must be logical"
  )


  # Download metadata
  meta_url <- paste0(
    paste(abs_api_url, "dataflow", "ABS", dataset, sep = "/"),
    "?references=all&detail=referencepartial&format=xml"
  )
  meta_path <- tempfile(fileext = ".xml")

  utils::download.file(
    meta_url,
    meta_path,
    method = method,
    mode = mode,
    quiet = TRUE
    )


  # Check and format dates
  date_range <- meta_date_range(meta_path)
  date_format <- meta_date_format(meta_path)

  if(!is.na(date_format)){
    max_date <- min(
      lubridate::as_date(max_date),
      lubridate::as_date(date_range[2])
    )
    min_date <- max(
      lubridate::as_date(min_date),
      lubridate::as_date(date_range[1])
    )
    min_date <- min(
      min_date,
      max_date
    )

    n_obs_pa <- meta_n_obs_pa(meta_path)
    n_years <- as.numeric((max_date - min_date) / 365)
    n_dates <- n_obs_pa * n_years
    if(n_dates < 1 | !is.finite(n_dates)) n_dates <- 1

    max_date <- as_abs_date(max_date, date_format)
    min_date <- as_abs_date(min_date, date_format)
  } else {
    n_dates <- 1
  }




  # Get metadata & column order (data key)
  meta <- read_abs_meta(meta_path, method = method, mode = mode)
  col_order <- meta_columns(meta_path)
  col_order$description <- meta$en[match(col_order$local_name, meta$id)]


  # Get constraints
  constraints <- meta_constraints(meta_path)


  # Generate dimension selection in data key order
  dimensions <- lapply(col_order$name, function(x){
    constraints$id_description[constraints$id == x]
  })
  names(dimensions) <- col_order$name


  # If filters are present, check, order, and update dimension selection
  if(length(filters) != 0){

    # Check filters are in data
    bad_filters <- !(names(filters) %in% unlist(col_order, use.names = FALSE))

    if(any(bad_filters)){
      warning(
        "The following dimentions were not found:\n",
        paste0(names(filters)[bad_filters], collapse = "\n")
      )
      filters <- filters[!bad_filters]
    }


    # Find positions in datakey
    filter_order <- sapply(names(filters), which_row, table = col_order)
    if(length(filter_order) != length(filters)) stop(
      "Filters match multiple dimension names"
    )


    # Validate filter contents (hacky - can be improved)
    filters <- lapply(1:length(filter_order), function(x){

      # Get valid dimension values as code or description
      lookup_table <- meta[
        meta$id == col_order$local_name[filter_order[x]],
        c("en_description", "id_description")
        ]


      # Find lookup table position of filter elements
      position <- sapply(filters[[x]], which_row, table = lookup_table)


      # Warn if bad filter element
      if(any(sapply(position, function(z) length(z) == 0))){
        bad_elements <- filters[[x]][
          sapply(position, function(z) length(z) == 0)
          ]
        warning(
          "'",
          col_order$description[filter_order[x]],
          "' dimension does not contain '",
          paste0(bad_elements, collapse = "', '"),
          "'"
          )
      }


      # Get valid dimension values
      valid_values <- lookup_table$id_description[unlist(position)]


      # Check against constraints
      constraint_check <- valid_values %in% constraints[
        constraints$id == col_order$name[filter_order[x]],
        "id_description"
        ]
      if(!all(constraint_check)){
        warning(
          "This dataset does not use ",
          col_order$name[filter_order[x]],
          ": '",
          paste0(valid_values[!constraint_check], collapse = "', '"),
          "'"
          )
        valid_values <- valid_values[constraint_check]
      }

      # Return
      return(valid_values)
    })


    # Update selected dimensions
    dimensions[filter_order] <- filters
  }


  # Estimate query size
  dim_lengths <- sapply(dimensions, length)
  query_size <- prod(dim_lengths) * n_dates
  req_n_splits <- ceiling(query_size / max_query_size)


  # Convert dimension selections to character string based on required n splits
  if(req_n_splits == 1){
    dim_strings <- paste0(
      sapply(dimensions, paste0, collapse = "+"),
      collapse = "."
    )
  } else if(prod(dim_lengths) >= req_n_splits){

    # Determine which dims to split on
    fully_split_dim_no <- min(which(cumprod(dim_lengths) > req_n_splits)) - 1
    partial_split_dim_no <- fully_split_dim_no + 1
    partial_split_n <- ceiling(
      req_n_splits / cumprod(dim_lengths[1:fully_split_dim_no])
      )


    # Split dims into required strings
    fully_split_dims <- dimensions[fully_split_dim_no] %iferror% NULL
    partial_split_dim <- dimensions[[partial_split_dim_no]]
    partial_split_dim <- split(
      partial_split_dim,
      cut(seq_along(partial_split_dim), partial_split_n, labels = FALSE)
      )
    non_split_dims <- dimensions[
      -c(fully_split_dim_no, partial_split_dim_no)
      ] %iferror% NULL


    # Convert vectors to strings
    partial_split_dim <- sapply(partial_split_dim, paste0, collapse = "+")
    non_split_dims <- lapply(non_split_dims, paste0, collapse = "+")


    # Generate request strings
    dim_strings <- expand.grid(
      c(
        fully_split_dims,
        list(partial_split_dim),
        non_split_dims
        )
      )

    dim_strings <- do.call(paste, c(as.list(dim_strings), sep = "."))

  } else {
    # Could also split on date - need to implement
    stop("Could not split data enough times to meet max_query_size")
  }


  # URL construction
  if(!is.na(date_format)){
    data_url <- paste0(
      abs_api_url,
      "/data/ABS,",
      dataset,
      "/",
      dim_strings,
      "?startPeriod=",
      min_date,
      "&endPeriod=",
      max_date,
      "&detail=",
      detail,
      "&format=csv"
    )
  } else {
    data_url <- paste0(
      abs_api_url,
      "/data/ABS,",
      dataset,
      "/",
      dim_strings,
      "?dimensionAtObservation=AllDimensions",
      "&detail=",
      detail,
      "&format=csv"
    )
  }



  # Download data
  data <- pbapply::pblapply(
    data_url,
    function(x){
      data.table::fread(x, showProgress = FALSE) %iferror% NULL
    },
    cl = cl
    )


  # Rowbind & unset data.table
  data <- data.table::rbindlist(data)
  data.table::setDF(data)


  # Reclass dates
  if("TIME_PERIOD" %in% colnames(data) & !is.na(date_format)){
    data$TIME_PERIOD <- parse_abs_date(data$TIME_PERIOD, date_format) %iferror%
      data$TIME_PERIOD
  }


  # Add names to codes
  code_names <- merge(
    meta[, c("id", "en_description", "id_description")],
    col_order[, c("local_name", "name")],
    by = 1L
  )[, -1]

  code_names <- split(code_names[, -3], code_names$name)
  code_names <- lapply(names(code_names), function(x){
    df <- code_names[[x]]
    names(df) <- c(paste0(x, "_name"), x)
    return(df)
  })

  for(i in 1:length(code_names)){
    data <- merge(
      x = data,
      y = code_names[[i]],
      all.x = TRUE,
      by = intersect(names(data), names(code_names[[i]]))
      )
  }


  # Correct unit scale
  if("UNIT_MULT" %in% colnames(data) & !scaled_units){
    data$OBS_VALUE <- ifelse(
      is.na(data$UNIT_MULT),
      data$OBS_VALUE,
      data$OBS_VALUE * (10 ^ data$UNIT_MULT)
      )
    data$UNIT_MULT <- NULL
  }


  # Remove dataflow
  if("DATAFLOW" %in% names(data)){
    data$DATAFLOW <- NULL
  }


  # Fix names & order
  reorder <- c(
    "TIME_PERIOD",
    col_order$name,
    "OBS_VALUE",
    "UNIT_MEASURE",
    paste0(col_order$name, "_name")
  )
  reorder <- reorder[reorder %in% names(data)]
  data <- data[, c(reorder, names(data)[!(names(data) %in% reorder)])]
  names(data) <- tolower(names(data))


  # Return data
  return(data)

}
