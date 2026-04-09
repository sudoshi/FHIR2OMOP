# Run OHDSI DataQualityDashboard against the BigQuery CDM.
#
# Usage:
#   Rscript run_dqd.R <project> <cdm_dataset> <vocab_dataset> [run_date]
#
# Output: JSON report under quality/output/<run_date>/results.json
#         (tail it into GCS for Looker Studio surfacing if you like)
#
# Prereqs (R packages):
#   install.packages(c("DatabaseConnector", "SqlRender"))
#   remotes::install_github("OHDSI/DataQualityDashboard")
#
# Also needs the BigQuery JDBC driver — point Sys.setenv(DATABASECONNECTOR_JAR_FOLDER)
# at the directory you downloaded it to.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: run_dqd.R <project> <cdm_dataset> <vocab_dataset> [run_date]")
}
project     <- args[[1]]
cdm_dataset <- args[[2]]
voc_dataset <- args[[3]]
run_date    <- if (length(args) >= 4) args[[4]] else format(Sys.Date(), "%Y-%m-%d")

library(DatabaseConnector)
library(DataQualityDashboard)

jar_folder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER", unset = "~/jdbc_drivers")
downloadJdbcDrivers("bigquery", pathToDriver = jar_folder)

connection_details <- createConnectionDetails(
  dbms            = "bigquery",
  user            = "",
  password        = "",
  connectionString = sprintf(
    "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=%s;OAuthType=3;",
    project
  ),
  pathToDriver = jar_folder
)

output_dir <- file.path("quality", "output", run_date)
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

executeDqChecks(
  connectionDetails         = connection_details,
  cdmDatabaseSchema         = sprintf("%s.%s", project, cdm_dataset),
  resultsDatabaseSchema     = sprintf("%s.%s", project, cdm_dataset),
  vocabDatabaseSchema       = sprintf("%s.%s", project, voc_dataset),
  cdmSourceName             = "Chile LIMS OMOP CDM",
  numThreads                = 4,
  sqlOnly                   = FALSE,
  outputFolder              = output_dir,
  outputFile                = "results.json",
  verboseMode               = FALSE,
  writeToTable              = FALSE,
  checkLevels               = c("TABLE", "FIELD", "CONCEPT"),
  cdmVersion                = "5.4",
  tablesToExclude           = c("CONCEPT","VOCABULARY","CONCEPT_ANCESTOR",
                                "CONCEPT_RELATIONSHIP","CONCEPT_CLASS",
                                "CONCEPT_SYNONYM","DOMAIN","DRUG_STRENGTH",
                                "RELATIONSHIP")
)

cat(sprintf("[done] DQD report at %s/results.json\n", output_dir))
