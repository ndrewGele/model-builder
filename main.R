# This script will build a model, save the model, and write metadata to db

library(dplyr)
library(dbplyr)

source(glue::glue('{Sys.getenv("COMMON_CODE_CONTAINER")}/src/get_all_data.R'))
source('./src/pick_model.R')


# Prerequisite Checks Before Running --------------------------------------

# Initialize S3 Connection and Bucket Info
s3 <- paws.storage::s3(
  config = list(
    credentials = list(anonymous = TRUE),
    endpoint = glue::glue(
      'http://', 
      Sys.getenv('SEAWEED_HOST'), 
      ':', 
      Sys.getenv('SEAWEED_S3_PORT')
    ),
    region = 'xx' # can't be blank
  )
)

bucket_names <- s3$list_buckets() |> 
  purrr::pluck('Buckets') |> 
  purrr::map_chr(purrr::pluck, 'Name')

# Create Bucket if it doesn't already exist
if(!Sys.getenv('SEAWEED_MODEL_BUCKET') %in% bucket_names) {
  s3$create_bucket(Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'))
  
  bucket_names <- s3$list_buckets() |> 
    purrr::pluck('Buckets') |> 
    purrr::map_chr(purrr::pluck, 'Name')
}

# Don't create models if directory is beyond a certain size
if(Sys.getenv('SEAWEED_MODEL_BUCKET') %in% bucket_names) {
  
  dir_limit <- as.numeric(Sys.getenv('SEAWEED_MODEL_LIMIT'))
  contents <- s3$list_objects(Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET')) |>
    purrr::pluck('Contents')
  if(length(contents) > 0) {
    dir_size <- contents |> 
      purrr::map_dbl(purrr::pluck, 'Size') |> 
      sum()
  } else {
    dir_size <- 0
  }
  
  message(glue::glue('Model object directory is 
                     {round(dir_size / dir_limit * 100, 2)}% full.'))
  
  if(dir_size > dir_limit) {
    message('Model directory is above the limit. 
            Sleeping for an hour before restarting.')
    Sys.sleep(60 * 60)
    stop('Done sleeping. Stopping process.')
  }
  
}

# Connect to DB
db_con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv('POSTGRES_DB'),
  host = Sys.getenv('POSTGRES_HOST'),
  port = Sys.getenv('POSTGRES_PORT'),
  user = Sys.getenv('POSTGRES_USER'),
  password = Sys.getenv('POSTGRES_PASSWORD')
)

# Ensure prerequisite tables exist in database
if(
  !DBI::dbExistsTable(db_con, 'symbols') |
  !DBI::dbExistsTable(db_con, 'daily_ohlc')
) {
  # Don't leave connection open while not using it
  DBI::dbDisconnect(db_con)
  stop('Missing one or more required tables. Stopping process.')
}


# Multi-thread for Model Tuning -------------------------------------------

cores_to_use <- parallelly::availableCores() /
  as.numeric(Sys.getenv('MODEL_CORES_FRACTION'))

cores_to_use |>
  ceiling() |> 
  max(1) |> 
  parallelly::makeClusterPSOCK() |>
  doParallel::registerDoParallel()


# Pick a Model, and Decide how to Update ----------------------------------

models_directory <- glue::glue('{Sys.getenv("MODELS_CONTAINER")}/src')
picked <- pick_model()


# Pull Data and Create Model ----------------------------------------------


# Pick a model to run, like how we picked symbols for other scripts


message(glue::glue('Picked {picked} randomly from available models.'))




message('Getting x_data.')
x_data <- get_all_data(db.con = db_con)




# Source Model Functions
message('Running model functions.')
if(exists(glue::glue(
  '{Sys.getenv("MODELS_CONTAINER")}/src/{picked}/{picked}_features.R'
))) {
  
  feat_instructions <- source(
    glue::glue(
      '{Sys.getenv("MODELS_CONTAINER")}/src/{picked}/{picked}_features.R'
    )
  )
  
}

model_fun <- source(glue::glue(
  '{Sys.getenv("MODELS_CONTAINER")}/src/{picked}/{picked}.R'
))$value
model_y_fun <- source(glue::glue(
  '{Sys.getenv("MODELS_CONTAINER")}/src/{picked}/{picked}_y.R'
))$value
model_results <- model_fun(
  x.data = x_data,
  db.con = db_con,
  y.fun = model_y_fun,
  cutoff.date = lubridate::ymd(Sys.getenv('MODEL_CUTOFF')),
  tune.initial = as.integer(Sys.getenv('MODEL_TUNE_INITIAL')),
  tune.iter = as.integer(Sys.getenv('MODEL_TUNE_ITER')),
  tune.no.improve = as.integer(Sys.getenv('MODEL_TUNE_NO_IMPROVE'))
)
message('Model function finished.')


# Save Model Obj and Write Data -------------------------------------------

# Use model function results to write model to S3
message('Parsing model results to save model objects.')
purrr::walk(
  .x = model_results,
  .f = function(x) {
    model_file_name <- glue::glue('{x$model_name}_{x$model_hash}.RDS')
    
    tmp <- tempfile(pattern = x$model_hash)
    saveRDS(
      object = x$model_obj |> 
        butcher::axe_call() |>
        butcher::axe_data() |> 
        butcher::axe_env(),
      file = tmp
    )
    
    s3$put_object(
      Body = tmp,
      Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'),
      Key = model_file_name
    )
  }
)
message('Model objects saved.')

# Use model function results to create database record
message('Parsing model results to write metadata to database.')
model_results_df <- purrr::map_dfr(
  .x = model_results,
  .f = function(x) {
    data.frame(
      name = x$model_name,
      recipe = x$model_recipe,
      model = x$model_model,
      feature_hash = x$feature_hash,
      training_perf = x$model_training_perf,
      holdout_perf = x$model_holdout_perf,
      file_name = glue::glue('{x$model_name}_{x$feature_hash}.RDS'),
      operation = picked$mode,
      update_timestamp = Sys.time()
    )
  }
)

if(!DBI::dbExistsTable(db_con, 'models')) {
  model_results_df |> 
    DBI::dbCreateTable(
      conn = db_con,
      name = 'models',
      fields = .
    )
  message('Models table created.')
}

model_results_df |> 
  DBI::dbAppendTable(
    conn = db_con,
    name = 'models',
    value = .
  )
message('Results written to models table.')


# Write feature data to feature lookup table
message('Parsing features.')
features_df <- purrr::map_dfr(
  .x = model_results,
  .f = function(x) {
    data.frame(
      feature_hash = x$feature_hash,
      feature_spec = test_tweaked_spec |> 
        as.character() |> 
        jsonlite::toJSON() |> 
        as.character()
    )
  }
) |> 
  distinct()

if(!DBI::dbExistsTable(db_con, 'features_lookup')) {
  features_df |> 
    DBI::dbCreateTable(
      conn = db_con,
      name = 'features_lookup',
      fields = .
    )
  message('Feature lookup table created.')
}

existing_features_df <- db_con |> 
  tbl('features_lookup') |> 
  distinct()

features_to_write <- features_df |> 
  anti_join(
    existing_features_df,
    by = 'feature_hash'
  )

features_to_write |> 
  DBI::dbAppendTable(
    conn = db_con,
    name = 'features_lookup',
    value = .
  )
message('Feature data written to feature lookup table.')


# End Process
DBI::dbDisconnect(db_con)

message('Process completed. Sleeping for two hours to cool down.')
Sys.sleep(60 * 60 * 2)
stop('Stopping process.')
