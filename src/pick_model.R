pick_model <- function(
  models.dir,
  db.con,
  models.table.name
) {
  
  # Initialize list that we'll return later
  picked <- list(feature_hash = 'N/A')
  
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
    picked$model <- sample(x = model_dirs_vec, size = 1)
    picked$mode <- 'new'
    
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
        'Models table exists, but not all have been created.\n',
        'Picking a random not-yet-created model.'
      )
      models_not_yet <- model_dirs_vec[
        !model_dirs_vec %in% unique(models_df$name)
      ]
      picked$model <- sample(x = models_not_yet, size = 1)
      picked$mode <- 'new'
      
    } else {
      
      message(
        'All models present in table',
        'Picking mode.'
      )
      # Determine whether to tweak, refresh, or try a new model
      picked_df <- models_df |> 
        slice_sample(n = 1)
      
      picked$model <- picked_df$name
      
      picked$mode <- sample(
        x = c('refresh', 'tweak', 'new'),
        size = 1
      )
      
      if(picked$mode != 'new') {
        picked$feature_hash <- picked_df$feature_hash
      }
      
    }
    
  }
  
  message(paste('Picked model:', picked$model))
  message(paste('Picked mode:', picked$mode))
  message(paste('Picked feature hash:', picked$feature_hash))
  
  return(picked)
  
}