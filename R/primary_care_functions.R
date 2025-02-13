# Take 


#' Find releases for the Appointments in General Practice dataset 
#'
#' @param url The url of the Appointments in General Practice dataset
#'
#' @returns A tibble with all the releases of the dataset
#' @export
#'
#' @examples
find_releases_primary_care <- function(
    url = "https://digital.nhs.uk/data-and-information/publications/statistical/appointments-in-general-practice" 
  ){
  
  # Get the raw HTML of the main page (always runs)
  main_html <- rvest::read_html(url)

  ### Find out when the latest release covers
  extract_latest_release_month <- function(html){
    my <- html |> 
      rvest::html_text2() |> 
      stringr::str_extract("Latest statistics\nAppointments in General Practice, (.*)\n") |> 
      stringr::str_extract(stringr::regex("(?<=Appointments in General Practice).*$", ignore_case = TRUE)) |> 
      stringr::str_trim() 
    
    lubridate::dmy(paste0("01", my))
  }
  
  latest_release <- extract_latest_release_month(main_html)
    
  
  ### Get the webpages of all the output that we want
    main_html_links <- main_html |> rvest::html_elements("a")
    
    link_info <- tibble::tibble(
      link = rvest::html_attr(main_html_links, "href")
      , title = stringr::str_trim(rvest::html_text(main_html_links))
    ) |> 
      dplyr::mutate(
        appts_in_gp = stringr::str_detect(title, stringr::regex("Appointments in General Practice", ignore_case = TRUE))
      )
    
    all_releases <- link_info |> 
      dplyr::filter(appts_in_gp) |> 
      dplyr::mutate(
        release_month = 
          lubridate::dmy(
            paste0(
              "01", 
              stringr::str_trim(title) |> 
                stringr::str_extract(stringr::regex("(?<=Appointments in General Practice).*$", ignore_case = TRUE)) |> 
                stringr::str_trim()
            )
          )
        , url = paste0("https://digital.nhs.uk", link)
      ) 
    
    current_releases <- all_releases |> 
      dplyr::filter(release_month <= latest_release) |> # Removes placeholders for future months
      dplyr::select(release_month, url)
    
    return(current_releases)
    
  
}

#' Parse links relating to the Appointments in General Practice dataset
#'
#' @param releases a tibble of urls of releases of appts in general practice
#'
#' @return a tibble with all links from the pages, highlighting which are downloads for specific purposes  
#' @export
#'
#' @examples
parse_links_primary_care <- function(releases){
  
    # Don't run this repeatedly - it's accessing all of the web pages
    all_pages <- releases |> 
      dplyr::mutate(html = purrr::map(url, rvest::read_html))
    
    get_links_df <- function(html){
      
      html_links <- html |> rvest::html_elements("a")
      
      tibble::tibble(
        link = rvest::html_attr(html_links, "href")
        , title = stringr::str_trim(rvest::html_text(html_links))
      )
      
    }
    
    all_the_links <- all_pages |> 
      dplyr::mutate(links = purrr::map(html, get_links_df)) |> 
      dplyr::select(-html) |> 
      tidyr::unnest(cols = links) 
    
    link_check <- all_the_links |>  
      dplyr::mutate(
        daily = stringr::str_detect(title, stringr::regex("daily", ignore_case = TRUE))
        , zip = stringr::str_detect(link, stringr::regex(".*zip$", ignore_case = TRUE))
        , appts_in_gp = stringr::str_detect(title, stringr::regex("Appointments in General Practice", ignore_case = TRUE))
        , summary = stringr::str_detect(title, stringr::regex("summary", ignore_case = TRUE))
        , xl = stringr::str_detect(link, stringr::regex(".*xlsx?$", ignore_case = TRUE))
        , annex = stringr::str_detect(title, stringr::regex("annex", ignore_case = TRUE))
      )
    
    return(link_check)
    # arrow::write_parquet(current_releases, here::here("data_raw", "primary_care", "pc_current_releases.parquet"))
    #arrow::write_parquet(link_check, here::here("data_raw", "primary_care", "pc_link_check.parquet"))
  }
  



#' Download files for primary care
#'
#' @param files_df A tibble of marked up links from the appointments in general practice dataset
#' @param results "daily" or "summary" Note that daily files are large and will be unzipped
#'
#' @return
#' @export
#'
#' @examples
download_files_primary_care <- function(files_df, results = "daily"){
  
  if(!dir.exists(here::here("data_raw", "primary_care"))) dir.create(here::here("data_raw", "primary_care"))
  
  if(results == "daily"){

    daily <- files_df |> 
      dplyr::filter(daily & zip) 

    # daily_check <- daily |>  
    #   dplyr::select(-url) |> 
    #   dplyr::right_join(pc_current_releases, by = "release_month")
     
    if(!dir.exists(here::here("data_raw", "primary_care", "daily"))) dir.create(here::here("data_raw", "primary_care", "daily"), recursive = TRUE)

    download_daily_files <- function(link){
      curl::curl_download(
        url = link
        , destfile = here::here("data_raw", "primary_care", "daily", basename(link))
      )  
    }
  
    purrr::walk(daily$link, download_daily_files)
    
    
    zip_manip <- daily |> 
      dplyr::mutate(file = basename(link)) |> 
      dplyr::select(release_month, file) 
    
      
    unzip_daily <- function(zip, folder){
      zip::unzip(
        zipfile = here::here("data_raw", "primary_care", "daily", zip)
        , exdir = here::here("data_raw", "primary_care", "daily", folder)
      )
    }
    
    purrr::walk2(zip_manip$file, zip_manip$release_month, unzip_daily)
    
    get_files <- function(folder){
      tibble::tibble(
        files = list.files(
          path = here::here("data_raw", "primary_care", "daily", folder)
          , recursive = TRUE
        )
      )
    }
    
    all_zip_contents <- zip_manip |> 
      dplyr::select(release_month) |> 
      dplyr::mutate(files = purrr::map(release_month, get_files)) |> 
      tidyr::unnest(cols = files)
    
    return(all_zip_contents)
    
  } else if(results == "summary"){
    
  if(!dir.exists(here::here("data_raw", "primary_care", "summary"))) dir.create(here::here("data_raw", "primary_care", "summary"))
  
  xl_summaries <- files_df |> 
    dplyr::filter(appts_in_gp & summary & xl & !annex)
  
  download_summaries <- xl_summaries |> 
    dplyr::mutate(file_name = basename(link))
  
  download_summary_files <- function(link){
    curl::curl_download(
      url = link
      , destfile = here::here("data_raw", "primary_care", "summary", basename(link))
    )  
  }
  
  purrr::walk(xl_summaries$link, download_summary_files)
  
  summary_out <- xl_summaries |> 
    dplyr::mutate(filename = here::here("data_raw", "primary_care", "summary", basename(link)))
  
  return(summary_out)
  
  } else {
    stop("Download parameter mis-specified")
  }
}

find_latest_files_primary_care <- function(pc_daily_files){
  # We need to look for the most recent version of each file:
  # Generate a list of all CSVs and the order they appeared in
  most_recent_daily <- pc_daily_files |> 
    dplyr::filter(!stringr::str_detect(files, stringr::regex("APPOINTMENTS_GP_COVERAGE.CSV", ignore_case = TRUE))) |>
    dplyr::mutate(file_dupe_name = basename(files)) |> 
    dplyr::group_by(file_dupe_name) |> 
    dplyr::arrange(dplyr::desc(release_month)) |> 
    dplyr::mutate(file_appearance = dplyr::row_number())
  
  # Read the most recent appearance for each month
  daily_csvs <- most_recent_daily |> 
    dplyr::filter(file_appearance == 1) |> 
    #Now create a flag to filter out duplicates due to file name change
    dplyr::mutate(
      appointment_month = lubridate::dmy(
        paste0(
          "01", 
          stringr::str_extract(file_dupe_name, "_([A-Za-z]+)[_]?([0-9]{2})\\.csv$") |> 
            stringr::str_remove("\\.csv")
        )
      )
    ) |> 
    dplyr::group_by(appointment_month) |> 
    dplyr::arrange(dplyr::desc(appointment_month)) |> 
    dplyr::mutate(month_appearance = dplyr::row_number()) |> 
    dplyr::filter(month_appearance == 1) |> 
    dplyr::select(-month_appearance)
  
  # Check for any missing months
  full_month_seq <- seq.Date(from = min(daily_csvs$appointment_month), to = max(daily_csvs$appointment_month), by = "month")
  
  missing_releases <- full_month_seq[!(full_month_seq %in% daily_csvs$appointment_month)]
  
  testthat::test_that(
    "No missing months", {
      testthat::expect_equal(length(missing_releases), 0)
    }
  )
  
  return(daily_csvs)
}


#' Clean coverage indicator
#'
#' @param pc_latest_daily a tibble with file paths from downloaded and unzipped appointments in general practice daily data
#'
#' @return
#' @export
#'
#' @examples
clean_pc_coverage <- function(pc_latest_daily){
  
  
  
  find_coverage <- function(release_month){
    list.files(
      path = here::here("data_raw", "primary_care", "daily", release_month)
      , pattern = "APPOINTMENTS_GP_COVERAGE.CSV"
      , full.names = TRUE
      , recursive = TRUE
      , include.dirs = TRUE
      , ignore.case = TRUE
    )
  }
  
  clean_appts_cols <- function(df){
    
    cn <- colnames(df)
    if("COMMISSIONER_ORGANISATION_CODE" %in% cn){
      out <- df |> 
        dplyr::rename(SUB_ICB_LOCATION_CODE = COMMISSIONER_ORGANISATION_CODE)
    } else {
      out <- df
    }
    
    janitor::clean_names(out) 
    
  }
  
  
  # Appointments coverage
  coverage_by_month <- pc_latest_daily |>
    dplyr::select(release_month, appointment_month) |> 
    dplyr::ungroup()
  
  coverage <- coverage_by_month |>
    dplyr::select(release_month) |>
    unique() |>
    dplyr::mutate(
      coverage_file = purrr::map(release_month, find_coverage)
      , data = purrr::map(coverage_file, readr::read_csv, show_col_types = FALSE)
    ) |>
    dplyr::mutate(data = purrr::map(data, clean_appts_cols))
  
  coverage_cols <- coverage |>
    dplyr::mutate(cols = purrr::map(data, colnames)) |>
    dplyr::select(release_month, cols) |>
    tidyr::unnest(cols = cols)

  coverage_combined <- coverage |>
    dplyr::select(release_month, data) |>
    tidyr::unnest(cols = data) |>
    dplyr::mutate(appointment_month = lubridate::dmy(appointment_month))

  coverage_narrow <- coverage_combined |>
    dplyr::select(release_month, sub_icb_location_code, appointment_month, included_practices,
           open_practices, patients_registered_at_included_practices,
           patients_registered_at_open_practices) |>
    dplyr::mutate(coverage_prop = patients_registered_at_included_practices / patients_registered_at_open_practices)

  coverage_to_join <- coverage_narrow |>
    dplyr::select(release_month, sub_icb_location_code, appointment_month, coverage_prop) |> 
    dplyr::right_join(coverage_by_month, by = c("release_month", "appointment_month"))
  
  # What a pain there are months without coverage information
  # dplyr::filter(coverage_to_join, is.na(coverage_prop))
  # release_month appointment_month
  # 2021-03-01   2018-10-01    
  # 2021-02-01   2018-09-01    
  # 2021-01-01   2018-08-01    
  # 2020-12-01   2018-07-01    
  # 2020-11-01   2018-06-01    
  # 2019-09-01   2018-03-01    
  # 2019-04-01   2017-11-01    

  return(coverage_to_join)
  
}

#' Clean daily primary care data
#'
#' Finds the most recent release for each month.
#' Reads each of these releases
#' Combines the releases into a single tibble
#' Adjusts the counts of appointments according to practice coverage
#'
#' @param pc_latest_daily a tibble of locations of daily files
#' @param pc_coverage a tibble of coverage adjustment by icb location code and month
#'
#' @return
#' @export
#'
#' @examples
clean_pc_daily <- function(pc_latest_daily, pc_coverage){
  
  daily_data <- pc_latest_daily |> 
    dplyr::mutate(
      full_path = here::here("data_raw", "primary_care", "daily", release_month, files)
      , data = purrr::map(full_path, readr::read_csv, show_col_types = FALSE)
    ) |> 
    dplyr::select(release_month, appointment_month, data) |> 
    tidyr::unnest(cols = data) |> 
    dplyr::ungroup() |> 
    janitor::clean_names() |> 
    dplyr::mutate(appointment_date = lubridate::dmy(appointment_date))
  
  full_ds <- daily_data |>
    dplyr::filter(appointment_month >= lubridate::ymd("2018-11-01")) |> # problems with coverage variable beforehand
    dplyr::select(release_month, appointment_month, sub_icb_location_code, appointment_date, appt_status
           , hcp_type, appt_mode, time_between_book_and_appt, count_of_appointments) |>
    dplyr::left_join(pc_coverage, by = c("release_month", "appointment_month", "sub_icb_location_code")) |>
    dplyr::mutate(count_of_appointments_adjusted = count_of_appointments / coverage_prop)

  return(full_ds)
  
}

#' Create the metrics for primary care data
#'
#' @param pc_daily_df A tibble of cleaned daily appointments in general practice data
#'
#' @return
#' @export
#'
#' @examples
metrics_pc_daily <- function(pc_daily_df){
  daily_summary <- pc_daily_df |> 
    dplyr::group_by(appointment_date) |>
    dplyr::summarise(
      appointments = sum(count_of_appointments)
      , appointments_adjusted = sum(count_of_appointments_adjusted)
    ) |> 
    dplyr::mutate(
      metric = "pc_appts_all"
    ) |> 
    # dplyr::select(date = appointment_date, metric, value = appointments_adjusted)
    dplyr::select(date = appointment_date, metric, value = appointments)
  
  same_day_summary <- pc_daily_df |>
    dplyr::filter(time_between_book_and_appt == "Same Day") |> 
    dplyr::group_by(appointment_date) |>
    dplyr::summarise(
      appointments = sum(count_of_appointments)
      , appointments_adjusted = sum(count_of_appointments_adjusted)
    ) |> 
    dplyr::mutate(
      metric = "pc_appts_same_day"
    ) |> 
    # dplyr::select(date = appointment_date, metric, value = appointments_adjusted)
    dplyr::select(date = appointment_date, metric, value = appointments)
  
  pc_metrics <- dplyr::bind_rows(
    daily_summary
    , same_day_summary
  )
  
  return(pc_metrics)
  
}

  
