# Packages
library(tidyverse)
library(sf)
library(cli)
library(progress)
library(osrm)

# Counties coordinates, from https://simplemaps.com/data/us-counties
counties <- read_csv(file = "counties_database/uscounties.csv") |>
  st_as_sf(coords = c("lng", "lat"), crs = 4326) |>
  select(county_fips)

# row.names(counties) <- counties$county_fips

# Cities pairs
counties_pairs <- combn(x = counties$county_fips, m = 2, simplify = F)
counties_pairs <- as.data.frame(matrix(unlist(counties_pairs), ncol = 2, byrow = TRUE))

# Matrix conversion
matrix_converter <- function(m, i, dest, name_var){
  # Tranpose
  m <- t(m)
  
  # To data frame
  m <- as.data.frame(m)
  
  # Add dest column
  m$dest <- dest
  
  # Add orig column
  m$orig <- st_drop_geometry(counties)[i,]$county_fips
  
  # Distance column name
  names(m) <- c(name_var, "dest", "orig")
  
  # Reorder
  m <- m[, c(3, 2, 1)]
  
  # Result
  return(m)
}

# Empty object to store results
# dist_usa <- data.frame()
# saveRDS(object = dist_usa, file = "dist_usa.rds", compress = FALSE)
# m <- 0
# saveRDS(object = m, file = "last_m.rds", compress = FALSE)

# Load last save
dist_usa <- readRDS("dist_usa.rds")

# Start query
for(m in 1:nrow(counties)){
  timestamp()
  cli_inform("County {m} from {nrow(counties)}")
  
  # Check against last query
  last_m <- readRDS("last_m.rds")
  if(m <= last_m){
    cli_alert_info("County {m} was already queried. Going to next one...")
    next
  } else {
    # Isolate county
    orig <- counties[m,]
    
    # Isolate destiny counties
    dest <- subset(counties_pairs, V1 == orig$county_fips)$V2
    
    # Creates a list of destination counties of size 200
    dest_chunk <- split(dest, ceiling(seq_along(dest)/200))
    
    # Progress bar
    pb <- progress_bar$new(
      format = "[:bar] :percent in :elapsed",
      total = length(dest_chunk), clear = FALSE, width= 60)
    
    for(n in 1:length(dest_chunk)){
      # Star progress bar
      pb$tick(0)
      
      # Destiny coordinates subset
      dest_tmp <- subset(counties, county_fips %in% dest_chunk[[n]])
      
      # Sleep to respect API restriction
      Sys.sleep(time = runif(1, 1, 2))
      
      # Get distance table for county i and counties package p
      tmp_res_pack <- osrmTable(
        src = orig, # Source
        dst = dest_tmp, # Destiny
        measure = c("distance", "duration") # Measures
      )
      
      # Isolate distance response
      tmp_res_pack_dist <- tmp_res_pack[["distances"]]
      tmp_res_pack_dist <- matrix_converter(m = tmp_res_pack_dist, 
                                            i = m,
                                            dest = dest_tmp$county_fips,
                                            name_var = "dist")
      
      # Isolate time response
      tmp_res_pack_dur <- tmp_res_pack[["durations"]]
      tmp_res_pack_dur <- matrix_converter(m = tmp_res_pack_dur, 
                                           i = m,
                                           dest = dest_tmp$county_fips,
                                           name_var = "dur")
      
      # Join responses
      tmp_res_pack_df <- merge(tmp_res_pack_dist, tmp_res_pack_dur)
      
      # Store responses on main data frame
      dist_usa <- rbind(dist_usa, tmp_res_pack_df)
      
      # Update progress bar
      pb$tick()
    }
    
    # Save after concluding all chunks
    saveRDS(object = dist_usa, file = "dist_usa.rds", compress = FALSE)
    saveRDS(object = m, file = "last_m.rds", compress = FALSE)
  }
  
  
}
