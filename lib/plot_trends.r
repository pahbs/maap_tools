library(ggplot2)
library(dplyr)
library(tidyr)
library(gridExtra)
library(scales)
library(terra)
library(sf)      # For spatial data handling
library(stars)   # For raster to sf conversion
library(ggspatial)


# Define color mappings and class labels
cmap_colors_kendall_classes <- c(
  "0" = "transparent",
  "1" = "#0000CC",    # Strong sig. positive - dark blue
  "2" = "#4D4DE6",    # Moderate sig. positive - medium blue
  "3" = "#8080FF",    # Weak sig. positive - light blue
  "4" = "#B3B3FF",    # Very weak sig. positive - very light blue
  "5" = "#CCCCCC",    # Non-sig. positive - light gray
  "6" = "#999999",    # Non-sig. negative - dark gray
  "7" = "#FFB3B3",    # Very weak sig. negative - very light red
  "8" = "#FF8080",    # Weak sig. negative - light red
  "9" = "#E64D4D",    # Moderate sig. negative - medium red
  "10" = "#CC0000"    # Strong sig. negative - dark red
)

class_labels_kendall_classes <- c(
  "1" = "Strong sig. [+]",
  "2" = "Mod. sig. [+]",
  "3" = "Weak sig. [+]",
  "4" = "Very weak sig. [+]",
  "5" = "Non-sig. [+]",
  "6" = "Non-sig. [-]",
  "7" = "Very weak sig. [-]",
  "8" = "Weak sig. [-]",
  "9" = "Mod. sig. [-]",
  "10" = "Strong sig. [-]"
)

# create_summary_statistics_figure <- function(tau_raster_path, p_raster_path, class_raster_path, output_path = NULL, alpha = 0.05) {
create_summary_statistics_figure <- function(raster_path, class_raster_path, output_path = NULL, alpha = 0.05) {
    
  # Read raster files
  cat("Reading raster files...\n")
  tau_raster <- rast(raster_path)[['kendall_tau']]
  p_raster <- rast(raster_path)[['kendall_pvalue']]
  class_raster <- rast(class_raster_path)
  
  # Convert to arrays/vectors
  tau_array <- values(tau_raster)
  p_array <- values(p_raster)
  class_array <- values(class_raster)
  
  # Remove NaN/NA values
  valid_mask <- !is.na(tau_array) & !is.na(p_array) & !is.na(class_array)
  valid_tau <- tau_array[valid_mask]
  valid_p <- p_array[valid_mask]
  valid_classes <- class_array[valid_mask]
  
  if (length(valid_tau) == 0) {
    cat("No valid Kendall's Tau results found\n")
    return(NULL)
  }
  
  cat(paste("Processing", length(valid_tau), "valid pixels...\n"))
  
  # 1. Histogram of Kendall's Tau values
  p1 <- ggplot(data.frame(tau = valid_tau), aes(x = tau)) +
    geom_histogram(fill = "steelblue", color = "black", alpha = 0.7) +
    geom_vline(xintercept = 0, color = "red", linetype = "dashed", size = 1) +
    labs(title = "Distribution of Kendall's Tau Values",
         x = "Kendall's Tau",
         y = "Frequency") +
    theme_minimal() +
    theme(plot.title = element_text(size = 12))
  
  # 2. Histogram of p-values
  p2 <- ggplot(data.frame(p_val = valid_p), aes(x = p_val)) +
    geom_histogram(fill = "forestgreen", color = "black", alpha = 0.7) +
    geom_vline(xintercept = alpha, color = "red", linetype = "dashed", size = 1) +
    annotate("text", x = alpha * 1.1, y = Inf, label = paste0("α=", alpha), 
             color = "red", size = 4, vjust = 1.5) +
    labs(title = "Distribution of p-values",
         x = "p-value",
         y = "Frequency") +
    theme_minimal() +
    theme(plot.title = element_text(size = 12))
  
  # 3. Histogram of classes
  p3 <- ggplot(data.frame(classes = valid_classes), aes(x = classes)) +
    geom_histogram(bins = 10, fill = "black", color = "black", alpha = 0.7) +
    labs(title = "Distribution of Kendall's tau classes",
         x = "Class",
         y = "Frequency") +
    theme_minimal() +
    theme(plot.title = element_text(size = 12))
  
  # 4. Pie chart of trend directions
  positive_trends <- sum(valid_tau > 0)
  negative_trends <- sum(valid_tau < 0)
  no_trends <- sum(valid_tau == 0)
  
  direction_data <- data.frame(
    direction = c("Positive", "Negative", "No trend"),
    count = c(positive_trends, negative_trends, no_trends)
  ) %>%
    filter(count > 0) %>%
    mutate(percentage = round(count/sum(count)*100, 1))
  
  p4 <- ggplot(direction_data, aes(x = "", y = count, fill = direction)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    scale_fill_manual(values = c("Positive" = "steelblue", 
                                "Negative" = "indianred", 
                                "No trend" = "lightgray")) +
    geom_text(aes(label = paste0(percentage, "%")), 
              position = position_stack(vjust = 0.5)) +
    labs(title = "Trend Directions") +
    theme_void() +
    theme(legend.title = element_blank(),
          plot.title = element_text(size = 12))
  
  # 5. Pie chart of significance and strength
  abs_tau <- abs(valid_tau)
  strong_sig <- sum(abs_tau >= 0.7 & valid_p < alpha)
  moderate_sig <- sum(abs_tau >= 0.4 & abs_tau < 0.7 & valid_p < alpha)
  weak_sig <- sum(abs_tau >= 0.2 & abs_tau < 0.4 & valid_p < alpha)
  very_weak_sig <- sum(abs_tau < 0.2 & valid_p < alpha)
  non_sig <- sum(valid_p >= alpha)
  
  strength_data <- data.frame(
    strength = c("Strong sig.", "Moderate sig.", "Weak sig.", "Very weak sig.", "Non-sig."),
    count = c(strong_sig, moderate_sig, weak_sig, very_weak_sig, non_sig)
  ) %>%
    filter(count > 0) %>%
    mutate(percentage = round(count/sum(count)*100, 1))
  
  p5 <- ggplot(strength_data, aes(x = "", y = count, fill = strength)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    geom_text(aes(label = paste0(percentage, "%")), 
              position = position_stack(vjust = 0.5)) +
    labs(title = "Trend Significance and Strength") +
    theme_void() +
    theme(legend.title = element_blank(),
          plot.title = element_text(size = 12))
  
    # 6. NEW: Pie chart of classified Kendall's tau values (SORTED)
    # Define class labels and colors
    class_labels_kendall_classes <- c(
    "1" = "Strong sig. [+]",
    "2" = "Mod. sig. [+]",
    "3" = "Weak sig. [+]",
    "4" = "Very weak sig. [+]",
    "5" = "Non-sig. [+]",
    "6" = "Non-sig. [-]",
    "7" = "Very weak sig. [-]",
    "8" = "Weak sig. [-]",
    "9" = "Mod. sig. [-]",
    "10" = "Strong sig. [-]"
    )
    
    cmap_colors_kendall_classes <- c(
    "1" = "#0000CC",
    "2" = "#4D4DE6",
    "3" = "#8080FF",
    "4" = "#B3B3FF",
    "5" = "#CCCCCC",
    "6" = "#999999",
    "7" = "#FFB3B3",
    "8" = "#FF8080",
    "9" = "#E64D4D",
    "10" = "#CC0000"
    )
    
    # Count occurrences of each class
    class_counts <- table(valid_classes)
    
    # Create dataframe for plotting with proper sorting
    classes_df <- data.frame(
    class = names(class_counts),
    count = as.numeric(class_counts)
    ) %>%
    filter(count > 0) %>%  # Remove empty classes
    mutate(class = as.character(class),
           class_num = as.numeric(class)) %>%  # Convert to numeric for sorting
    arrange(class_num) %>%  # Sort by class number (1, 2, 3, ..., 10)
    mutate(percentage = round(count/sum(count)*100, 1),
           label = ifelse(class %in% names(class_labels_kendall_classes), 
                         class_labels_kendall_classes[class], 
                         paste("Class", class)),
           # Create ordered factor to maintain sort order in ggplot
           class_ordered = factor(class, levels = class)) %>%
    select(-class_num)  # Remove helper column
    
    # Create color mapping for present classes (in sorted order)
    present_classes <- classes_df$class
    color_values <- cmap_colors_kendall_classes[present_classes]
    names(color_values) <- present_classes
    
    # Create pie chart of classified values (sorted)
    p6 <- ggplot(classes_df, aes(x = "", y = count, fill = class_ordered)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    scale_fill_manual(values = color_values,
                     labels = class_labels_kendall_classes[present_classes],
                     name = "Trend Classes") +
    geom_text(aes(label = paste0(percentage, "%")), 
              position = position_stack(vjust = 0.5),
              size = 3) +
    labs(title = "Trend Classes") +
    theme_void() +
    theme(legend.title = element_text(size = 10),
          legend.text = element_text(size = 9),
          plot.title = element_text(size = 12))
  
  # Combine all plots in a 3x2 grid
  combined_plot <- grid.arrange(p1, p2, p3, p4, p5, p6, 
                               ncol = 3, nrow = 2,
                               top = "Summary Statistics of Kendall's Tau Analysis")
  
  # Save if output path provided
  if (!is.null(output_path)) {
    ggsave(output_path, combined_plot, width = 15, height = 10, dpi = 300)
    cat(paste("Summary statistics figure saved to:", output_path, "\n"))
  }
  
  # Print summary statistics
  cat("\n", paste(rep("=", 50), collapse = ""), "\n")
  cat("KENDALL'S TAU SUMMARY STATISTICS\n")
  cat(paste(rep("=", 50), collapse = ""), "\n")
  cat(paste("Total valid pixels:", length(valid_tau), "\n"))
  cat(paste("Mean τ:", round(mean(valid_tau), 4), "\n"))
  cat(paste("Median τ:", round(median(valid_tau), 4), "\n"))
  cat(paste("Range: [", round(min(valid_tau), 4), ", ", round(max(valid_tau), 4), "]\n", sep = ""))
  cat(paste("Significant pixels (p<", alpha, "): ", sum(valid_p < alpha), 
            " (", round(100*sum(valid_p < alpha)/length(valid_p), 1), "%)\n", sep = ""))
  
  return(combined_plot)
}

raster_read_proj_convert <- function(fn, to_crs="EPSG:4326", BOREAL_MAP=TRUE, LYR=NA){
    
    raster_proj <- rast(fn)    
    r <- project(raster_proj, to_crs , method='cubic' )
    
    # Convert raster to data frame
    r_df <- as.data.frame(r, xy = TRUE)
    
    if(BOREAL_MAP){
        names(r_df)[names(r_df) == 'lyr1'] <- 'prediction mean'
        names(r_df)[names(r_df) == 'sd']   <- 'prediction standard deviation'
        }
    if(!is.na(LYR)){
        names(r_df) <- c('x','y', LYR)
        }
    return (r_df)
    }

theme_custom = list(
      theme_bw() +
      theme(
          axis.text.y = element_text(angle=90),
          legend.position="top",
          legend.key.width=unit(4, "cm"),
          legend.key.height=unit(0.75, "cm"),
          legend.text=element_text(size=rel(1.5)),
          legend.title=element_text(size=rel(2))
      ),
          annotation_scale(location = "tl", style="ticks") ,
          #annotation_scale(location = "bl", style="box") ,
          annotation_north_arrow(location = "br", which_north = "true", style=north_arrow_minimal())
    )

crs_canalb = '+proj=aea +lat_1=50 +lat_2=70 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'


plot_classified_kendall_map <- function(class_raster_path, output_path = NULL, 
                                      figsize = c(12, 12), dpi = 300,
                                      class_labels = class_labels_kendall_classes,
                                      cmap_colors = cmap_colors_kendall_classes) {
  
  # Read raster file
  cat("Reading classified raster file...\n")
  class_raster <- rast(class_raster_path)
  #class_raster <- raster_read_proj_convert(class_raster_path, to_crs="EPSG:4326", BOREAL_MAP=FALSE, LYR='class_kendall_tau')
  
  # Convert terra raster to stars object for better sf integration
  class_stars <- st_as_stars(class_raster)
  
  # Convert stars to sf data frame
  class_sf <- st_as_sf(class_stars, as_points = FALSE, merge = TRUE)
  
  # Clean up the data frame
  # The column name will be the original raster name or a generic name
  value_col <- names(class_sf)[1]  # First column should be the values
  names(class_sf)[1] <- "class"
  
  # Remove NA values and class 0 (no data)
  class_sf <- class_sf[!is.na(class_sf$class) & class_sf$class != 0, ]
  class_sf$class <- factor(class_sf$class)
  print(head(class_sf))
  cat(paste("Plotting", nrow(class_sf), "valid pixels...\n"))
  
  # Filter colors and labels to only include classes present in data
  present_classes <- levels(class_sf$class)
  filtered_colors <- cmap_colors[present_classes]
  filtered_labels <- class_labels[present_classes]
  
  # Create the plot using geom_sf
  p <- ggplot(class_sf) +
    geom_sf(aes(fill = class), color = NA, size = 0) +  # color = NA removes borders
    scale_fill_manual(values = filtered_colors,
                     labels = filtered_labels,
                     name = "Trend Classes") +
    coord_sf(crs=crs_canalb, expand = FALSE) +  # Remove extra space around plot
    labs(title = paste0(basename(class_raster_path),"\nclassified kendall's tau trend analysis")) +
    theme_bw() +
    theme(
      plot.title = element_text(size = 14, hjust = 0.5),
      legend.position = "right",
      legend.title = element_text(size = 8),
      legend.text = element_text(size = 6),
      legend.background = element_rect(fill = "white"), #, alpha = 0.8),
      panel.background = element_rect(fill = "white"),
      panel.grid = element_blank(),
      axis.text = element_blank(),
      axis.ticks = element_blank()
    ) +
    guides(fill = guide_legend(override.aes = list(alpha = 1))) +
    theme_custom

  # Save if output path provided
  if(!is.null(output_path)) {
    ggsave(output_path, p, width = figsize[1], height = figsize[2], dpi = dpi, units = "in")
    cat(paste("Classified map saved to:", output_path, "\n"))
  }
  
  print(p)
  return(p)
}