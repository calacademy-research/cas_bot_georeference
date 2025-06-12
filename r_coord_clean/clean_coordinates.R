# clean_coordinates.R

# Install required packages if missing
if (!requireNamespace("CoordinateCleaner", quietly = TRUE)) {
  install.packages("CoordinateCleaner", repos = "http://cran.us.r-project.org")
}
if (!requireNamespace("countrycode", quietly = TRUE)) {
  install.packages("countrycode", repos = "http://cran.us.r-project.org")
}

suppressPackageStartupMessages({
  library(CoordinateCleaner)
  library(countrycode)
})

# Parse command-line arguments
args <- commandArgs(trailingOnly = TRUE)
input_file <- args[1]
output_file <- args[2]

# Read input
df <- read.csv(input_file, stringsAsFactors = FALSE)

# Ensure proper types
df$latitude <- as.numeric(df$latitude)
df$longitude <- as.numeric(df$longitude)
df$country_iso3 <- as.character(countrycode(df$country, origin = "country.name", destination = "iso3c"))

# Run CoordinateCleaner
result <- clean_coordinates(df,
                            lon = "longitude",
                            lat = "latitude",
                            countries = "country_iso3",
                            species = NULL,
                            tests = c("seas", "zeros", "equal", "centroids")  # Only non-GBIF/institution tests for speed and stability
)

# Add result flag
df$cc_valid <- result$.summary

# Write output
write.csv(df, output_file, row.names = FALSE)
