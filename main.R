# This script will build a model, save the model, and write metadata to db

library(dplyr)
library(dbplyr)

source(file.path(
  Sys.getenv('COMMON_CODE_CONTAINER'),
  'src',
  'get_all_data.R'
))
source(file.path(
  Sys.getenv('COMMON_CODE_CONTAINER'),
  'src',
  'feature_utils.R'
))
source(file.path(
  '.', 
  'src', 
  'pick_model.R'
))


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
} else {
  message('Found all required tables.')
}


# Pick a Model and Decide how to Update -----------------------------------

models_directory <- glue::glue('{Sys.getenv("MODELS_CONTAINER")}/src')
picked <- pick_model(
  models.dir = models_directory,
  db.con = db_con,
  models.table.name = 'models'
)


# Get Data ----------------------------------------------------------------

message('Getting base training data.')
df <- get_all_data(db.con = db_con)

message('Checking for features.')
if(
  file.exists(
    file.path(
      Sys.getenv('MODELS_CONTAINER'),
      'src',
      picked$model,
      glue::glue('{picked$model}_features.R')
    )
  )
) {
  
  message('Features found, creating feature definition.')
  feature_definition <- source(
    file.path(
      Sys.getenv('MODELS_CONTAINER'),
      'src',
      picked$model,
      glue::glue('{picked$model}_features.R')
    )
  )$value
  
  message('Creating feature spec based on mode.')
  if(picked$mode == 'new') {
  
    feature_spec <- generate_feature_spec(feature_definition)
  
  } else {
    
    message('Pulling previous feature spec from database.')
    
    existing_features_df <- db_con |> 
      tbl('features_lookup') |> 
      filter(model_name == !!picked$model) |> 
      collect()
    
    prev_feature_spec <- existing_features_df |> 
      pull(feature_spec_string) |> 
      sample(size = 1) |> 
      jsonlite::fromJSON() |>
      purrr::map(\(x) eval(parse(text = x)))
    
    if(picked$mode == 'refresh') {
      message('Refreshing previous feature spec.')
      feature_spec <- prev_feature_spec
    }
    
    if(picked$mode == 'tweak') {
      message('Tweaking previous feature spec.')
      feature_spec <- tweak_feature_spec(prev_feature_spec, feature_definition)
    }
    
  }
  
  feature_hash <- feature_spec |> 
    purrr::map(purrr::pluck, 'name') |> 
    unlist() |>
    sort() |>
    rlang::hash() |> 
    substr(1, 8)
  
  feature_spec_string <- feature_spec |> 
    as.character() |> 
    jsonlite::toJSON() |> 
    as.character()
  
  features_df <- create_features(df, feature_spec)
  df <- bind_cols(df, features_df)
  
}

message('Pulling Y Data')

model_y_fun <- source(file.path(
  Sys.getenv("MODELS_CONTAINER"),
  'src',
  picked$model,
  glue::glue('{picked$model}_y.R')
))$value

y_data <- model_y_fun(db.con = db_con)

df <- inner_join(
  y_data,
  df,
  by = c('symbol', 'date')
) |> 
  ungroup()

message('Training data is ready.')


# Multi-thread for Model Tuning -------------------------------------------

cores_to_use <-  as.numeric(parallelly::availableCores()) *
  as.numeric(Sys.getenv('MODEL_CORES_FRACTION'))

# Ceiling() gets its own line because 
# it was always returning the same as availableCores() for some reason??
cores_to_use <- ceiling(cores_to_use)

if(cores_to_use > 1) {
  library(future)
  plan(multisession, workers = cores_to_use)
}


# Run Model Training ------------------------------------------------------

model_fun <- source(file.path(
  Sys.getenv("MODELS_CONTAINER"),
  'src',
  picked$model,
  glue::glue('{picked$model}.R')
))$value

message('Starting model function.')
model_results <- model_fun(
  data = df,
  cutoff.date = lubridate::ymd(Sys.getenv('MODEL_CUTOFF')),
  tune.initial = as.integer(Sys.getenv('MODEL_TUNE_INITIAL')),
  tune.iter = as.integer(Sys.getenv('MODEL_TUNE_ITER')),
  tune.no.improve = as.integer(Sys.getenv('MODEL_TUNE_NO_IMPROVE'))
)
message('Model function finished.')


# Save Feature and Model Data ---------------------------------------------

# Feature Data
message('Saving feature hash mapping to database.')
feature_lookup_df <- data.frame(
  feature_hash = feature_hash,
  feature_spec_string = feature_spec_string
)

if(!DBI::dbExistsTable(db_con, 'features_lookup')) {
  feature_lookup_df |> 
    DBI::dbCreateTable(
      conn = db_con,
      name = 'features_lookup',
      fields = _
    )
  message('Features table created.')
} 

existing_features_df <- db_con |> 
  tbl('features_lookup') |> 
  distinct() |> 
  collect()

feature_lookup_df <- feature_lookup_df |> 
  anti_join(
    existing_features_df,
    by = 'feature_hash'
  )

if(nrow(feature_lookup_df) > 0) {
  feature_lookup_df |> 
    DBI::dbAppendTable(
      conn = db_con,
      name = 'features_lookup',
      value = _
    )
  message('Feature data written to feature lookup table.')
} else {
  message('No new feature data to write to database.')
}

# Write model object to S3
message('Parsing model results to save model objects.')
purrr::walk(
  .x = model_results,
  .f = function(x) {
    
    tmp <- tempfile()
    
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
      Key = paste0(
        x$model_name, '_',
        x$model_recipe, '_',
        x$model_engine, '_',
        feature_hash,
        '.RDS'
      )
    )
    
  }
)
message('Model objects saved.')

# Write model metadata to database
message('Parsing model results to write metadata to database.')
model_results_df <- purrr::map_dfr(
  .x = model_results,
  .f = function(x) {
    data.frame(
      name = x$model_name,
      recipe = x$model_recipe,
      engine = x$model_engine,
      feature_hash = feature_hash,
      training_perf = x$model_training_perf,
      holdout_perf = x$model_holdout_perf,
      file_name = paste0(
        x$model_name, '_',
        x$model_recipe, '_',
        x$model_engine, '_',
        feature_hash,
        '.RDS'
      ),
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
      fields = _
    )
  message('Models table created.')
}

model_results_df |> 
  DBI::dbAppendTable(
    conn = db_con,
    name = 'models',
    value = _
  )
message('Results written to models table.')


# End Process -------------------------------------------------------------

DBI::dbDisconnect(db_con)

message('Process completed. Sleeping for two hours to cool down.')
Sys.sleep(60 * 60 * 2)
stop('Stopping process.')
