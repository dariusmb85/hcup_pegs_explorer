library(arrow)
library(dplyr)
library(readr)

# Load PheCode mapping and create lookup
phecode_map <- read_csv("phecodes_cm_rolled.csv")
phecode_lookup <- setNames(phecode_map$phecode, phecode_map$code)

# Your new combined formatting function
format_icd_for_phecode <- function(code) {
  if(is.na(code) || code == "") return(NA_character_)

  # ICD-10 detection: starts with letter
  if(grepl("^[A-Z]", code)) {
    if(nchar(code) == 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  # ICD-9 E-codes: decimal after 4th position if 5+ chars
  if(grepl("^E[0-9]", code)) {
    if(nchar(code) <= 4) return(code)
    if(nchar(code) > 4) {
      return(paste0(substr(code, 1, 4), ".", substr(code, 5, nchar(code))))
  }
}
  # ICD-9 V-codes: decimal after 3rd position if 4+ chars
  if(grepl("^V[0-9]", code)) {
    if(nchar(code) <= 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  # ICD-9 numeric codes: decimal after 3rd position if 4+ chars
  if(grepl("^[0-9]+$", code)) {
    if(nchar(code) == 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  return(code)
}

# Check PheCodes by year
check_year_phecodes <- function(year) {
  cat("Checking", year, "...\n")

  files <- list.files("../../../UT/SID/", pattern = paste0(year, ".*\\.parquet$"), full.names = TRUE)
  cat("Found files:", paste(files, collapse = ", "), "\n")

  all_phecodes <- c()
  total_icds <- 0

  for(file in files) {
    visits <- read_parquet(file)
    # Handle both ICD-9 (DX1-DX30) and ICD-10 (I10_DX1-I10_DX30) naming
    dx_cols <- names(visits)[grepl("^DX[0-9]+$|^I10_DX[0-9]+$", names(visits))]
    cat("DX columns found:", length(dx_cols), "\n")


    for(col in dx_cols) {
      icds <- visits[[col]][!is.na(visits[[col]])]
      total_icds <- total_icds + length(icds)

      for(icd in icds[1:min(10, length(icds))]) {  # Test first 10 only
        formatted_icd <- format_icd_for_phecode(icd)
        cat("ICD:", icd, "→ Formatted:", formatted_icd, "\n")
        if(!is.na(formatted_icd) && formatted_icd %in% names(phecode_lookup)) {
          phecode <- phecode_lookup[[formatted_icd]]
          cat("  → PheCode:", phecode, "\n")
          all_phecodes <- c(all_phecodes, phecode)
        }
      }
      break  # Just test first column for now
    }
    break  # Just test first file for now
  }

  cat("Total ICDs processed:", total_icds, "\n")
  cat("PheCodes found:", length(all_phecodes), "\n")

  if(length(all_phecodes) == 0) {
    return(data.frame(year = year, phecode = character(0), count = numeric(0)))
  }

  # Count unique PheCodes
  phecode_counts <- table(all_phecodes)
  result <- data.frame(
    year = year,
    phecode = names(phecode_counts),
    count = as.numeric(phecode_counts)
  ) %>% arrange(desc(count))

  cat("Found", nrow(result), "unique PheCodes\n")
  return(result)
}

# Check PheCodes for multiple years
years <- 2010:2020
all_results <- list()

for(year in years) {
  cat("Processing year", year, "...\n")
  tryCatch({
    all_results[[as.character(year)]] <- check_year_phecodes(year)
  }, error = function(e) {
    cat("Error with year", year, ":", e$message, "\n")
    all_results[[as.character(year)]] <- NULL
  })
}

# Access individual results
results_2010 <- all_results[["2010"]]
results_2011 <- all_results[["2011"]]
results_2012 <- all_results[["2012"]]
results_2013 <- all_results[["2013"]]
results_2014 <- all_results[["2014"]]
results_2015 <- all_results[["2015"]]
results_2016 <- all_results[["2016"]]
results_2017 <- all_results[["2017"]]
results_2018 <- all_results[["2018"]]
results_2019 <- all_results[["2019"]]
results_2020 <- all_results[["2020"]]

# Show which years have data
cat("Years with data:\n")
for(year in years) {
  result <- all_results[[as.character(year)]]
  if(!is.null(result) && nrow(result) > 0) {
    cat(year, ": ", nrow(result), " unique PheCodes\n")
  } else {
    cat(year, ": No data\n")
  }
}

# Simple year-by-year PheCode listing
cat("=== PheCodes Found by Year ===\n\n")

for(year in 2010:2020) {
  result <- all_results[[as.character(year)]]

  if(!is.null(result) && nrow(result) > 0) {
    phecodes_list <- paste(result$phecode, collapse = ", ")
    cat(year, ":", phecodes_list, "\n")
  } else {
    cat(year, ": No data\n")
  }
}

# Show top 20 per year
print("Top 20 PheCodes by year:")
print(head(results_2012, 20))