# Set up

# remotes::install_github("OHDSI/DatabaseConnector")
# remotes::install_github("OHDSI/FeatureExtraction", ref = "cohortCovariates")

outputLocation <- file.path("D:", "test")
unlink(x = outputLocation, recursive = TRUE)
dir.create(outputLocation, recursive = TRUE)

targetCohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble()

outcomeCohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = 23)

# cohort definition set
featureCohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds =
                                               PhenotypeLibrary::getPhenotypeLog() |>
                                               dplyr::filter(stringr::str_detect(string = tolower(hashTag), pattern = "symptoms")) |>
                                               dplyr::pull(cohortId) |>
                                               unique())

cohortDefinitionSet <-
  dplyr::bind_rows(targetCohortDefinitionSet,
                   outcomeCohortDefinitionSet,
                   featureCohortDefinitionSet)

database <- "eunomia"
resultsLocation <- file.path(outputLocation, database)
cohortDatabaseSchema <- "main"
cdmDatabaseSchema <- "main"
databaseId <- database
cohortTableNames <-
  CohortGenerator::getCohortTableNames(cohortTable = "cohort")
targetCohortDatabaseSchema <- cdmDatabaseSchema
targetCohortTable <- cohortTableNames$cohortTable
outcomeCohortDatabaseSchema <- cdmDatabaseSchema
outcomeCohortTable <- cohortTableNames$cohortTable
featureCohortDatabaseSchema <- cohortDatabaseSchema
featureCohortTable <- cohortTableNames$cohortTable

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
CohortGenerator::createCohortTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames
)
CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet
)

# feature extraction specification
cohortDiagnosticsCovariateSettings <-
  CohortDiagnostics::getDefaultCovariateSettings()

cohortBasedCovariateSettings <-
  FeatureExtraction::createCohortBasedTemporalCovariateSettings(
    analysisId = 150,
    covariateCohortDatabaseSchema = featureCohortDatabaseSchema,
    covariateCohortTable = featureCohortTable,
    covariateCohorts = featureCohortDefinitionSet |>
      dplyr::select(cohortId, cohortName),
    valueType = "binary",
    temporalStartDays = cohortDiagnosticsCovariateSettings$temporalStartDays,
    temporalEndDays = cohortDiagnosticsCovariateSettings$temporalEndDays
  )

covariateSettings <- list(cohortDiagnosticsCovariateSettings, cohortBasedCovariateSettings)

# Setting for characterization package
aggregateCovariateSettings <- Characterization::createAggregateCovariateSettings(
  targetIds = targetCohortDefinitionSet$cohortId,
  outcomeIds = outcomeCohortDefinitionSet$cohortId,
  riskWindowStart = 0,
  startAnchor = 'cohort start',
  riskWindowEnd = 0,
  endAnchor = 'cohort end',
  covariateSettings = covariateSettings
)

characterizationSettings <- Characterization::createCharacterizationSettings(
  aggregateCovariateSettings = list(
    aggregateCovariateSettings
  )
)

outputLocation <- Characterization::runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  targetDatabaseSchema = targetCohortDatabaseSchema,
  targetTable = targetCohortTable,
  outcomeDatabaseSchema = outcomeCohortDatabaseSchema,
  outcomeTable = outcomeCohortTable,
  characterizationSettings = characterizationSettings,
  saveDirectory = resultsLocation,
  tablePrefix = 'c_',
  databaseId = databaseId
)

