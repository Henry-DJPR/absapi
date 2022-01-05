

#' Get ABS metadata
#'
#' @param dataset Single length character string containing dataset name,
#' metadata URL or downloaded metadata xml file
#' @param structure_type Data structure. One of datastructure, dataflow,
#' codelist, conceptscheme, categoryscheme, contentconstraint, actualconstraint,
#' agencyscheme, categorisation, hierarchicalcodelist
#' @param references Determines which artefacts referenced by the artefact are
#' returned. One of none, parents, parentsandsiblings, children, descendants,
#' all.
#' @param detail Desired amount of detail to be returned. One of allstubs,
#' referencestubs, referencepartial, allcompletestubs, referencecompletestubs,
#' full.
#' @param method Method to be used for downloading files. Current download
#' methods are "internal", "wininet" (Windows only) "libcurl", "wget" and
#' "curl", and there is a value "auto": see ‘Details’ and ‘Note’ in
#' `download.file()`. The method  can also be set through the option
#'  "download.file.method": see `options()`.
#' @param mode character. The mode with which to write the file. Useful values
#' are "w", "wb" (binary), "a" (append) and "ab". Not used for methods
#' "wget" and "curl".
#'
#' @return A dataframe of requested ABS metadata
#' @export
#'
#' @examples
#' meta <- read_abs_meta("MERCH_EXP")
#'
read_abs_meta <- function(
  dataset,
  structure_type = "dataflow",
  references = "all",
  detail = "referencepartial",
  method = "libcurl",
  mode = "w"
  ){

  # Get objects from options
  abs_api_url <- getOption("abs_api_url", "https://api.data.abs.gov.au")


  # Match arguments
  structure_type <- match.arg(
    arg = structure_type,
    choices = c(
      "dataflow",
      "datastructure",
      "codelist",
      "conceptscheme",
      "categoryscheme",
      "contentconstraint",
      "actualconstraint",
      "categorisation",
      "hierarchicalcodelist"
      )
  )

  references <- match.arg(
    arg = references,
    choices = c(
      "none",
      "parents",
      "parentsandsiblings",
      "children",
      "descendants",
      "all"
    )
  )

  detail <- match.arg(
    arg = detail,
    choices = c(
      "referencepartial",
      "allstubs",
      "allstubs",
      "allcompletestubs",
      "referencecompletestubs",
      "full"
    )
  )


  # Check inputs
  if(length(abs_api_url) != 1 | !("character" %in% class(abs_api_url))) stop(
    "abs_api_url option must be a single length character"
  )

  if(length(dataset) != 1 | !("character" %in% class(dataset))) stop(
    "Dataset must be a single length character"
  )


  # Determine if path, URL or dataset name and assign sdmx_input accordingly
  if(grepl("^http[s]?://", dataset) | file.exists(dataset)){

    sdmx_input <- dataset

  } else {

    sdmx_input <- paste0(
      paste(abs_api_url, structure_type, "ABS", dataset, sep = "/"),
      paste0("?references=", references, "&detail=", detail, "&format=xml")
    )

  }


  # Download metadata
  meta <- readsdmx::read_sdmx(
    sdmx_input,
    quiet = TRUE,
    method = method,
    mode = mode
    )


  # Remove superfluous columns
  meta$agencyID <- NULL


  # Return
  return(meta)
}
