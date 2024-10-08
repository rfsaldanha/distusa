library(readr)
library(arrow)

dist_usa <- readRDS("dist_usa.rds")

dist_usa$dest <- as.numeric(dist_brasil$dest)

saveRDS(object = dist_usa, file = "export/dist_usa_compressed.rds", compress = "xz")

write_csv2(x = dist_usa, file = "export/dist_usa.csv")

write_parquet(x = dist_usa, sink = "export/dist_usa.parquet")
