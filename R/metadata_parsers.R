meta_constraints <- function(path){

  # Parse XML constraints
  if("xml_node" %in% class(path)){

    constr <- xml2::xml_find_all(path, ".//structure:Constraints")

  } else if(file.exists(path) | grepl("^http[s]?://", path)){

    constr <- xml2::xml_find_all(
      xml2::read_xml(path),
      ".//structure:Constraints"
    )

  } else {

    stop("Input is not a path, URL or xml_node class object")

  }


  # Extract constraint applicable dates
  constr_date_to <- xml2::xml_attr(xml2::xml_children(constr),"validTo")
  constr_date_from <- xml2::xml_attr(xml2::xml_children(constr), "validFrom")

  constr_date_to <- lubridate::as_date(constr_date_to)
  constr_date_from <- lubridate::as_date(constr_date_from)

  constr_date_to <- constr_date_to > Sys.time()
  constr_date_from <- constr_date_from < Sys.time()

  constr_date_to[is.na(constr_date_to)] <- FALSE
  constr_date_from[is.na(constr_date_from)] <- FALSE

  current <- seq_len(xml2::xml_length(constr))[
    constr_date_to | constr_date_from
  ]

  if(length(current) != 1) stop(
    "Found too few or too many constraint sets"
  )


  # Get constraints as table
  constr <- xml2::xml_child(
    xml2::xml_child(constr, current),
    "structure:CubeRegion"
    )

  constr_df <- lapply(xml2::xml_children(constr), function(x) data.frame(
    id = xml2::xml_attr(x, "id"),
    id_description = xml2::xml_text(xml2::xml_children(x))
  ))


  # Return dataframe
  return(do.call(rbind, constr_df))

}




# Returns a date format based on constraints in meta data file

meta_date_format <- function(path){

  # Parse XML constraints
  if("xml_node" %in% class(path)){

    constr <- xml2::xml_find_all(path, ".//structure:Constraints")

  } else if(file.exists(path) | grepl("^http[s]?://", path)){

    constr <- xml2::xml_find_all(
      xml2::read_xml(path),
      ".//structure:Constraints"
    )

  } else {

    stop("Input is not a path, URL or xml_node class object")

  }


  # Extract constraint applicable dates
  constr_date_to <- xml2::xml_attr(xml2::xml_children(constr),"validTo")
  constr_date_from <- xml2::xml_attr(xml2::xml_children(constr), "validFrom")

  constr_date_to <- lubridate::as_date(constr_date_to)
  constr_date_from <- lubridate::as_date(constr_date_from)

  constr_date_to <- constr_date_to > Sys.time()
  constr_date_from <- constr_date_from < Sys.time()

  constr_date_to[is.na(constr_date_to)] <- FALSE
  constr_date_from[is.na(constr_date_from)] <- FALSE

  current <- seq_len(xml2::xml_length(constr))[
    constr_date_to | constr_date_from
  ]

  if(length(current) != 1) stop(
    "Found too few or too many constraint sets"
  )


  # Get frequency
  constr <- xml2::xml_child(constr, current)
  freq <- xml2::xml_text(
    xml2::xml_child(constr,"structure:CubeRegion/common:KeyValue[@id='FREQ']")
  )


  # Return function
  return(freq)

}



# Returns min and max dates
meta_date_range <- function(path){

  # Parse XML constraints
  if("xml_document" %in% class(path)){

    constr <- xml2::xml_find_all(path, ".//structure:Constraints")

  } else if(file.exists(path) | grepl("^http[s]?://", path)){

    constr <- xml2::xml_find_all(
      xml2::read_xml(path),
      ".//structure:Constraints"
    )

  } else {

    stop("Input is not a path, URL or xml_document class object")

  }


  # Extract constraint applicable dates
  constr_date_to <- xml2::xml_attr(xml2::xml_children(constr),"validTo")
  constr_date_from <- xml2::xml_attr(xml2::xml_children(constr), "validFrom")

  constr_date_to <- lubridate::as_date(constr_date_to)
  constr_date_from <- lubridate::as_date(constr_date_from)

  constr_date_to <- constr_date_to > Sys.time()
  constr_date_from <- constr_date_from < Sys.time()

  constr_date_to[is.na(constr_date_to)] <- FALSE
  constr_date_from[is.na(constr_date_from)] <- FALSE

  current <- seq_len(xml2::xml_length(constr))[
    constr_date_to | constr_date_from
  ]

  if(length(current) != 1) stop(
    "Found too few or too many constraint sets"
  )


  # Get date range
  constr <- xml2::xml_child(constr, current)
  date_range <- xml2::xml_text(
    xml2::xml_child(
      constr,
      "structure:CubeRegion/common:KeyValue[@id='TIME_PERIOD']"
      )
  )


  # Format as dates
  min_date <- lubridate::as_date(substr(date_range, 1, 19))
  max_date <- lubridate::as_date(substr(date_range, 20, 38))


  # Return dates
  return(c(min_date, max_date))

}



# Returns column names in order
meta_columns <- function(path){

  # Parse XML constraints
  if("xml_document" %in% class(path)){

    structure <- xml2::xml_child(
      path,
      paste(
        "message:Structures",
        "structure:DataStructures",
        "structure:DataStructure",
        "structure:DataStructureComponents",
        "structure:DimensionList",
        sep = "/"
      )
    )

  } else if(file.exists(path) | grepl("^http[s]?://", path)){

    structure <- xml2::xml_child(
      xml2::read_xml(path),
      paste(
        "message:Structures",
        "structure:DataStructures",
        "structure:DataStructure",
        "structure:DataStructureComponents",
        "structure:DimensionList",
        sep = "/"
        )
    )

  } else {

    stop("Input is not a path, URL or xml_document class object")

  }


  # Check if multiple
  column_names <- xml2::xml_attr(xml2::xml_children(structure), "id")
  column_order <- xml2::xml_attr(xml2::xml_children(structure), "position")
  column_names_local <- xml2::xml_attr(
    xml2::xml_child(
      xml2::xml_children(structure),
      "structure:LocalRepresentation/structure:Enumeration/Ref"
    ),
    "id"
  )


  # Structure as data.frame and order
  column_info <- data.frame(
    name = column_names,
    local_name = column_names_local
  )

  column_info <- column_info[column_order, ]
  column_info <- column_info[stats::complete.cases(column_info), ]


  # Return columns in order
  return(column_info)

}




# Gets number of observations per annum
meta_n_obs_pa <- function(path){

  # Parse XML constraints
  if("xml_node" %in% class(path)){

    constr <- xml2::xml_find_all(path, ".//structure:Constraints")

  } else if(file.exists(path) | grepl("^http[s]?://", path)){

    constr <- xml2::xml_find_all(
      xml2::read_xml(path),
      ".//structure:Constraints"
    )

  } else {

    stop("Input is not a path, URL or xml_node class object")

  }


  # Extract constraint applicable dates
  constr_date_to <- xml2::xml_attr(xml2::xml_children(constr),"validTo")
  constr_date_from <- xml2::xml_attr(xml2::xml_children(constr), "validFrom")

  constr_date_to <- lubridate::as_date(constr_date_to)
  constr_date_from <- lubridate::as_date(constr_date_from)

  constr_date_to <- constr_date_to > Sys.time()
  constr_date_from <- constr_date_from < Sys.time()

  constr_date_to[is.na(constr_date_to)] <- FALSE
  constr_date_from[is.na(constr_date_from)] <- FALSE

  current <- seq_len(xml2::xml_length(constr))[
    constr_date_to | constr_date_from
  ]

  if(length(current) != 1) stop(
    "Found too few or too many constraint sets"
  )


  # Get frequency
  constr <- xml2::xml_child(constr, current)
  freq <- xml2::xml_text(
    xml2::xml_child(constr,"structure:CubeRegion/common:KeyValue[@id='FREQ']")
  )


  # Date frequency
  date_frequency <- switch(
    freq,
    A = 1L,
    M = 12L,
    D = 365L,
    B = 365L,
    W = 52L,
    S = 2L,
    Q = 4L,
    N = stop("Minutely data not implimented"),
    H = stop("Hourly data not implimented"),
    stop("Date format not found")
  )


  # Return function
  return(date_frequency)

}
