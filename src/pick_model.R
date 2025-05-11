pick_model <- function(
  models.dir,
  db.con,
  models.table.name,
) {
  
  # Initialized result
  picked <- list()
  
  # Get a clean vector of models from the directory provided
  model_dirs_vec <- list.dirs(
    path = models.dir, 
    full.names = FALSE
  )
  model_dirs_vec <- model_dirs_vec[model_dirs_vec != '']
  
  
  # Possibilities are:
  # If no models have been made, just pick one to make
  # If not all have been made, pick one from unmade models
  # If all models have been made once, choose randomly from the following:
  # Retrain on fresh data with same features
  # Retrain model with a new feature set
  # Retrain with a tweaked feature set (searching for optima)
  if(!DBI::dbExistsTable(db.con, models.table.name)) {
    
    message('No models table found, creating first model.')
    picked_model <- sample(x = model_dirs_vec, size = 1)
    picked_mode <- 'new'
    
  } else {
    
    # Logic for other possibilities requires model metadata
    models_df <- db.con |> 
      tbl(models.table.name) |> 
      inner_join(
        db.con |> 
          tbl(models.table.name) |> 
          group_by(name, feature_hash) |> 
          summarise(update_timestamp = max(update_timestamp, na.rm = TRUE)),
        by = c('name', 'feature_hash', 'update_timestamp')
      ) |> 
      collect()
    
    # If any models haven't been generated, we'll pick one of those
    if(any(!model_dirs_vec %in% models_df$name)) {
      
      message(
        'Models table exists, but not all have been created.',
        'Picking a random not-yet-created model.'
      )
      models_not_yet <- model_dirs_vec[!model_dirs_vec %in% models_df$name]
      picked <- sample(x = models_not_yet, size = 1)
      mode <- 'new'
      
    } else {
      
      # Determine whether to tweak, refresh, or try a new model
      
      
    }
    
  }
  
  
  # Return result: a list of two strings
  
  picked$model <- picked_model
  picked$mode <- picked_mode
  
  return(picked)
  
}