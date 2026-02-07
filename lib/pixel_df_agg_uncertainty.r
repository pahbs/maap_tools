library(dplyr)
library(ggplot2)
library(purrr)

# # Simulate tile-based pixel data with uniform pixel sizes
# create_tile_data <- function(n_tiles = 10, pixels_per_tile = 100, 
#                            STAT_mean = "STAT_mean", STAT_sd = "STAT_sd") {
  
#   # Create data for each tile - no area/size columns since all pixels are identical
#   tile_data <- map_dfr(1:n_tiles, function(tile_num) {
#     # Create base data frame
#     base_data <- data.frame(
#       tile_num = tile_num,
#       pixel_id = paste0(tile_num, "_", 1:pixels_per_tile),
#       temp_mean = rnorm(pixels_per_tile, 150 + tile_num * 5, 30),  # Slight tile effect
#       temp_sd = runif(pixels_per_tile, 10, 25),
#       age_class = sample(c("Young", "Mature", "Old"), pixels_per_tile, replace = TRUE),
#       # Tile-specific metadata (no pixel areas)
#       processing_date = Sys.Date() - sample(0:30, 1)  # Processing dates
#     )
    
#     # Rename columns to match input arguments
#     names(base_data)[names(base_data) == "temp_mean"] <- STAT_mean
#     names(base_data)[names(base_data) == "temp_sd"] <- STAT_sd
    
#     return(base_data)
#   })
  
#   return(tile_data)
# }

# Function to calculate uncertainty within each tile with pixel size specification
calculate_tile_uncertainty <- function(tile_data, STAT_mean = "STAT_mean", STAT_sd = "STAT_sd",
                                     pixel_size_m = 30, GROUP_COLS = c('tile_num','age_class','ecoregions_ECO_NAME')) {
  
  # Calculate pixel area in different units
  pixel_area_m2 <- pixel_size_m^2
  pixel_area_ha <- pixel_area_m2 / 10000  # Convert m² to hectares
  
  cat(paste("Using pixel size:", pixel_size_m, "m x", pixel_size_m, "m\n"))
  cat(paste("Pixel area:", pixel_area_m2, "m² (", pixel_area_ha, "ha per pixel)\n"))
  
  # Use sym() to create symbols for column names
  mean_col <- sym(STAT_mean)
  sd_col <- sym(STAT_sd)
  
  tile_data %>%
    group_by_at(GROUP_COLS) %>%
    summarise(
      n_pixels = n(),
      #processing_date = first(processing_date),
      
      # Calculate tile area from pixel count and pixel size
      tile_area_m2 = n_pixels * pixel_area_m2,
      tile_area_ha = n_pixels * pixel_area_ha,
      
      # Tile-level summaries by age class using dynamic column names
      total_STAT_mean = sum(!!mean_col, na.rm = TRUE),
      total_STAT_variance = sum((!!sd_col)^2, na.rm = TRUE),
      total_STAT_sd = sqrt(total_STAT_variance),
      
      # Per-unit-area estimates
      STAT_per_m2_mean = total_STAT_mean / tile_area_m2,
      STAT_per_ha_mean = total_STAT_mean / tile_area_ha,
      STAT_per_m2_sd = total_STAT_sd / tile_area_m2,
      STAT_per_ha_sd = total_STAT_sd / tile_area_ha,
      
      # Coefficient of variation
      cv = total_STAT_sd / total_STAT_mean * 100,
      
      # 95% CI for tile
      ci_lower = total_STAT_mean - 1.96 * total_STAT_sd,
      ci_upper = total_STAT_mean + 1.96 * total_STAT_sd,
      
      # Add pixel size info for reference
      pixel_size_m = pixel_size_m,
      pixel_area_m2 = pixel_area_m2,
      pixel_area_ha = pixel_area_ha,
      
      .groups = 'drop'
    )
}

# Function to aggregate across tiles (updated to handle calculated areas)
aggregate_tile_uncertainty <- function(tile_summaries, GROUP_COLS = c('age_class','ecoregions_ECO_NAME')) {
  
  # Method A: Simple aggregation (assumes tile independence)
  simple_aggregation <- tile_summaries %>%
    group_by_at(GROUP_COLS) %>%
    summarise(
      n_tiles = n(),
      total_pixels = sum(n_pixels),
      pixel_size_m = first(pixel_size_m),
      pixel_area_ha = first(pixel_area_ha),
      
      # Calculate total domain area from pixel counts
      total_area_m2 = sum(tile_area_m2),
      total_area_ha = sum(tile_area_ha),
      total_area_km2 = total_area_ha / 100,  # Convert to km²
      
      # Aggregate STAT across tiles
      domain_total_STAT_mean = sum(total_STAT_mean),
      domain_total_STAT_variance = sum(total_STAT_variance),
      domain_total_STAT_sd = sqrt(domain_total_STAT_variance),
      
      # Domain-wide per-unit-area estimates
      domain_STAT_per_m2_mean = domain_total_STAT_mean / total_area_m2,
      domain_STAT_per_ha_mean = domain_total_STAT_mean / total_area_ha,
      domain_STAT_per_m2_sd = domain_total_STAT_sd / total_area_m2,
      domain_STAT_per_ha_sd = domain_total_STAT_sd / total_area_ha,
      
      # Overall coefficient of variation
      domain_cv = domain_total_STAT_sd / domain_total_STAT_mean * 100,
      
      # 95% CI for domain
      domain_ci_lower = domain_total_STAT_mean - 1.96 * domain_total_STAT_sd,
      domain_ci_upper = domain_total_STAT_mean + 1.96 * domain_total_STAT_sd,
      
      # Relative standard error
      rse_percent = (domain_total_STAT_sd / domain_total_STAT_mean) * 100,
      
      .groups = 'drop'
    )
  
  # Method B: Accounting for inter-tile variability
  inter_tile_variability <- tile_summaries %>%
    group_by_at(GROUP_COLS) %>%
    summarise(
      # Statistics across tiles
      mean_tile_STAT = mean(total_STAT_mean),
      sd_tile_STAT = sd(total_STAT_mean),
      
      # Combined uncertainty (within-tile + between-tile)
      within_tile_variance = mean(total_STAT_variance),
      between_tile_variance = var(total_STAT_mean) * n(),  # Scale by n tiles
      
      # Total variance combining both sources
      total_combined_variance = sum(total_STAT_variance) + between_tile_variance,
      total_combined_sd = sqrt(total_combined_variance),
      
      .groups = 'drop'
    )
  
  return(list(
    simple = simple_aggregation,
    with_inter_tile = inter_tile_variability
  ))
}

# Function for bootstrap uncertainty across tiles (unchanged)
bootstrap_tile_uncertainty <- function(tile_data, STAT_mean = "STAT_mean", STAT_sd = "STAT_sd", 
                                     n_bootstrap = 1000, GROUP_COLS = c('tile_num','age_class','ecoregions_ECO_NAME')) {
  
  # Create symbols for column names
  mean_col <- sym(STAT_mean)
  sd_col <- sym(STAT_sd)
  
  bootstrap_results <- replicate(n_bootstrap, {
    
    # For each bootstrap iteration
    tile_data %>%
      group_by(tile_num) %>%

      # Sample pixels within each tile (with replacement)
      sample_n(size = n() , replace = TRUE) %>%
      ungroup() %>%
      # Sample from each pixel's distribution using dynamic column names
      mutate(
        sampled_STAT = rnorm(n(), mean = !!mean_col, sd = !!sd_col)
      ) %>%
      # Aggregate by age class
      group_by(age_class) %>%
      summarise(total_STAT = sum(sampled_STAT), .groups = 'drop')
    
  }, simplify = FALSE)
  
  # Combine bootstrap results
  bootstrap_summary <- map_dfr(bootstrap_results, ~.x, .id = "iteration") %>%
    group_by(age_class) %>%
    summarise(
      bootstrap_mean = mean(total_STAT),
      bootstrap_sd = sd(total_STAT),
      bootstrap_ci_lower = quantile(total_STAT, 0.025),
      bootstrap_ci_upper = quantile(total_STAT, 0.975),
      .groups = 'drop'
    )
  
  return(bootstrap_summary)
}

# Function to work with existing data
analyze_existing_tile_data <- function(tile_data, STAT_mean, STAT_sd, pixel_size_m = 30, CONVERT_Mg_to_Pg=TRUE) {
  
  # Validate that columns exist
  if (!STAT_mean %in% names(tile_data)) {
    stop(paste("Column", STAT_mean, "not found in data"))
  }
  if (!STAT_sd %in% names(tile_data)) {
    stop(paste("Column", STAT_sd, "not found in data"))
  }
  
  cat(paste("Analyzing existing data with columns:", STAT_mean, "and", STAT_sd, "\n"))
  cat(paste("Using pixel size:", pixel_size_m, "m x", pixel_size_m, "m\n"))

  if(CONVERT_Mg_to_Pg){
      cat("Converting to Pg...")
      # Use sym() to create symbols for column names
      mean_col <- sym(STAT_mean)
      sd_col <- sym(STAT_sd)
      # Total carbon in Pg (1 Pg = 10^9 Mg)
      Mg_to_Pg = 1e-9
      tile_data = tile_data %>% mutate(
                !!sym(STAT_mean) := !!sym(STAT_mean) * Mg_to_Pg ,
                !!sym(STAT_sd) := !!sym(STAT_sd) * Mg_to_Pg
          )
      }
  
  # Calculate tile-level uncertainty
  tile_summaries <- calculate_tile_uncertainty(tile_data, STAT_mean, STAT_sd, pixel_size_m)
  
  # Aggregate across tiles
  domain_summaries <- aggregate_tile_uncertainty(tile_summaries)
  
  # Bootstrap uncertainty
  bootstrap_results <- bootstrap_tile_uncertainty(tile_data, STAT_mean, STAT_sd, n_bootstrap = 10)
  
  return(list(
    tile_summaries = tile_summaries,
    domain_simple = domain_summaries$simple,
    domain_with_inter_tile = domain_summaries$with_inter_tile,
    bootstrap_results = bootstrap_results,
    column_names = list(mean_col = STAT_mean, sd_col = STAT_sd),
    pixel_size_m = pixel_size_m
  ))
}

# Visualization function that handles dynamic column names
create_flexible_plots <- function(results, analysis_title = "Biomass Analysis", UNITS='Pg') {
  
  mean_col_name <- results$column_names$mean_col
  sd_col_name <- results$column_names$sd_col
  
  # Plot 1: Domain-wide comparison with dynamic title
  # #...bootpstrap is off...  
  # comparison_data = results$domain_simple %>% 
  #     dplyr::select(age_class, mean = domain_total_STAT_mean, 
  #            sd = domain_total_STAT_sd) %>%
  #     mutate(method = "Simple Aggregation")

  # bootstrap on...
  comparison_data <- bind_rows(
    results$domain_simple %>% 
      dplyr::select(age_class, mean = domain_total_STAT_mean, 
             sd = domain_total_STAT_sd) %>%
      mutate(method = "Simple Aggregation")
      ,
    results$bootstrap_results %>%
      dplyr::select(age_class, mean = bootstrap_mean, sd = bootstrap_sd) %>%
      mutate(method = "Bootstrap")
  )
  
  p1 <- ggplot(comparison_data, aes(x = age_class, y = mean, fill = method)) +
    geom_col(position = "dodge") +
    geom_errorbar(aes(ymin = mean - 1.96*sd, ymax = mean + 1.96*sd),
                  position = position_dodge(width = 0.9), width = 0.2) +
    labs(title = paste(analysis_title, ": Method Comparison"),
         subtitle = paste("Data columns:", mean_col_name, "and", sd_col_name),
         x = "Age Class", y = paste0("Total (",UNITS,")"),
         fill = "Method") +
    theme_minimal()
  
  return(p1)
}
