#' Read compressed .dat files
#' 
#' .dat files are fixed width files column information stored in
#' corresponding .fts files. 
#' `readr::read_fwf` automatically closes the connection
read_dat <- function(tar, file, start, names = NULL, types = NULL, n = Inf, skip = 0, col_select = NULL) {
  if (!is.null(tar)) {
      con <- unz(tar, file)
  } else {
      con <- file
  }
  
  readr::read_fwf(con, 
                  readr::fwf_widths(start, col_names = names), 
                  show_col_types = FALSE, 
                  col_types = types, 
                  n_max = n, 
                  skip = skip, 
                  skip_empty_rows = FALSE, 
                  col_select = col_select, 
                  trim_ws = TRUE, 
                  na = "")
}

read_lines_zip <- function(tar, file, n = -1L) {
  con <- unz(tar, file)
  on.exit(close(con))
  readLines(con, n = n)
}

#' Path to 12201 raw data directory
path_12201 <- "/mnt/data/disabilityandpain-r/12201"
path_12692 <- "/mnt/data/disabilityandpain-r/moud/12692"

number_of_rows <- function(tar, file) {
    if (!is.null(tar)) {files <- unzip(tar, list = TRUE)$Name
        meta <- try(read_lines_zip(tar, files[which(endsWith(files, ".fts"))]))
        return(readr::parse_number(meta[grep("Exact File Quantity", meta)]))
    }
    
    meta <- try(readLines(file))
    readr::parse_number(meta[grep("Exact File Quantity", meta)])
}

# From https://stackoverflow.com/a/27626007/10046836
chunk <- function(x, n) {
  mapply(function(a, b) (x[a:b]), 
         seq.int(from = 1, to = length(x), by = n), 
         pmin(seq.int(from = 1, to = length(x), by = n) + (n - 1), 
              length(x)), 
         SIMPLIFY = FALSE)
}
