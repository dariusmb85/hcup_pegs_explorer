source(here::here("r", "00_env.R"))

cfg <- yaml::read_yaml(here::here("config", "datasets.yaml"))

for (nm in names(cfg$datasets)) {
  ds_cfg <- cfg$datasets[[nm]]

  amadeus::download_data(
    nm,
    out_dir = paths$am_cache,
    version = ds_cfg$version
  )

  amadeus::process_covariates(
    nm,
    cache_dir = paths$am_cache,
    crs = ds_cfg$crs
  )
}