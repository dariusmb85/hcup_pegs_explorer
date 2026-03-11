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