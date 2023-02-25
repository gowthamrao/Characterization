# Set up

remotes::install_github("OHDSI/DatabaseConnector")
remotes::install_github("OHDSI/FeatureExtraction", ref = 'cohortCovariates')

outputLocation <- file.path("D:", "studyResults", "experiment", "rapidPhenotyping")

# connection details and data source
database <- 'optum_extended_dod'
connectionDetails <-
  createConnectionDetailsLocal(database = database)
cdmSource <- getCdmSource(database = database)

resultsLocation <- file.path(outputLocation, cdmSource$sourceId)
cohortDatabaseSchema <- cdmSource$cohortDatabaseSchemaFinal
cdmDatabaseSchema <- cdmSource$cdmDatabaseSchemaFinal
outcomeDatabaseSchema <- cohortDatabaseSchema

# cohort tables
cohortTableNames <-
  CohortGenerator::getCohortTableNames(cohortTable = paste0(
    stringr::str_squish("pl_"),
    stringr::str_squish(cdmSource$sourceKey)
  ))

targetCohortDatabaseSchema <- cohortDatabaseSchema
targetCohortTable <- cohortTableNames$cohortTable
outcomeCohortDatabaseSchema <- cohortDatabaseSchema
outcomeCohortTable <- cohortTableNames$cohortTable
featureCohortDatabaseSchema <- cohortDatabaseSchema
featureCohortTable <- cohortTableNames$cohortTable

Characterizations <- c(23) # inpatient stay
targetCohortIds <- c(259) # anaphylaxis

# cohort definition set
log <-
  dplyr::bind_rows(
    PhenotypeLibrary::getPhenotypeLog() |>
      dplyr::filter(stringr::str_detect(string = tolower(hashTag), pattern = "symptoms")),  # feature cohorts
    PhenotypeLibrary::getPhenotypeLog() |>
      dplyr::filter(cohortId %in% c(Characterizations, targetCohortIds)))
cohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = log$cohortId)


# feature extraction specification
cohortDiagnosticsCovariateSettings <-
  CohortDiagnostics::getDefaultCovariateSettings()

cohortBasedCovariateSettings <-
  FeatureExtraction::createCohortBasedTemporalCovariateSettings(
    analysisId = 150,
    covariateCohortDatabaseSchema = featureCohortDatabaseSchema,
    covariateCohortTable = featureCohortTable,
    covariateCohorts = log |>
      dplyr::select(cohortId, cohortName),
    valueType = "binary",
    temporalStartDays = cohortDiagnosticsCovariateSettings$temporalStartDays,
    temporalEndDays = cohortDiagnosticsCovariateSettings$temporalEndDays
  )


# Setting for characterization package
aggregateCovariateSettings1 <- Characterization::createAggregateCovariateSettings(
  targetIds = targetCohortIds,
  outcomeIds = Characterizations,
  riskWindowStart = 0,
  startAnchor = 'cohort start',
  riskWindowEnd = 0,
  endAnchor = 'cohort end',
  covariateSettings = cohortDiagnosticsCovariateSettings
)
aggregateCovariateSettings2 <- Characterization::createAggregateCovariateSettings(
  targetIds = targetCohortIds,
  outcomeIds = Characterizations,
  riskWindowStart = 0,
  startAnchor = 'cohort start',
  riskWindowEnd = 0,
  endAnchor = 'cohort end',
  covariateSettings = cohortBasedCovariateSettings
)

timeToEventSettings1 <- Characterization::createTimeToEventSettings(
  targetIds = targetCohortIds,
  outcomeIds = Characterizations
)
dechallengeRechallengeSettings <- Characterization::createDechallengeRechallengeSettings(
  targetIds = targetCohortIds,
  outcomeIds = Characterizations,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 31
)


characterizationSettings <- Characterization::createCharacterizationSettings(
  timeToEventSettings = list(
    timeToEventSettings1
  ),
  dechallengeRechallengeSettings = list(
    dechallengeRechallengeSettings
  ),
  aggregateCovariateSettings = list(
    aggregateCovariateSettings1,
    aggregateCovariateSettings2
  )
)


Characterization::runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  targetDatabaseSchema = targetCohortDatabaseSchema,
  targetTable = targetCohortTable,
  outcomeDatabaseSchema = outcomeCohortDatabaseSchema,
  outcomeTable = outcomeCohortTable,
  characterizationSettings = characterizationSettings,
  saveDirectory = resultsLocation,
  tablePrefix = 'c_',
  databaseId = cdmSource$sourceKey
)
