# shiny/app.R (Enhanced)
library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)
library(plotly)
library(DT)
library(scales)

# Setup paths
root <- Sys.getenv("PARQUET_ROOT", "data")
exwas_path <- file.path(root, "gold", "exwas_result")
pm_path <- file.path(root, "gold", "person_month")
exposure_path <- file.path(root, "gold", "exposure_daily")

# Load datasets
exwas_ds <- open_dataset(exwas_path)
pm_ds <- open_dataset(pm_path)
exp_ds <- open_dataset(exposure_path)

# UI
ui <- fluidPage(
  theme = bslib::bs_theme(version = 5, bootswatch = "flatly"),

  titlePanel("PEGS Explorer — Environment × Health Analysis"),

  tabsetPanel(
    id = "main_tabs",

    # Tab 1: ExWAS Results
    tabPanel(
      "ExWAS Results",

      sidebarLayout(
        sidebarPanel(
          width = 3,

          h4("Filters"),

          selectInput(
            "model_spec",
            "Model Specification",
            choices = NULL,  # populated dynamically
            selected = NULL
          ),

          selectInput(
            "outcome",
            "Outcome (Disease)",
            choices = NULL,
            selected = NULL
          ),

          sliderInput(
            "qmax",
            "Max q-value (FDR)",
            min = 0, max = 1, value = 0.1, step = 0.01
          ),

          sliderInput(
            "pmax",
            "Max p-value",
            min = 0, max = 1, value = 0.05, step = 0.001
          ),

          hr(),

          h4("Display Options"),

          checkboxInput("show_labels", "Label significant points", TRUE),

          numericInput("label_top_n", "Label top N", value = 10, min = 1, max = 50),

          downloadButton("download_results", "Download Results", class = "btn-primary btn-sm")
        ),

        mainPanel(
          width = 9,

          fluidRow(
            column(
              6,
              h4("Volcano Plot"),
              plotlyOutput("volcano_plot", height = "500px")
            ),
            column(
              6,
              h4("Effect Size Distribution"),
              plotlyOutput("effect_dist", height = "500px")
            )
          ),

          hr(),

          h4("Results Table"),
          DTOutput("results_table")
        )
      )
    ),

    # Tab 2: Exposure Time Series
    tabPanel(
      "Exposure Explorer",

      sidebarLayout(
        sidebarPanel(
          width = 3,

          h4("Location"),

          selectInput(
            "geo_type",
            "Geography Type",
            choices = c("Tract" = "tract", "ZIP" = "zip5")
          ),

          textInput(
            "geo_id",
            "Geographic ID",
            placeholder = "e.g., 37063050100 or 27701"
          ),

          h4("Exposure"),

          selectInput(
            "exposure_id",
            "Exposure Variable",
            choices = NULL
          ),

          dateRangeInput(
            "date_range",
            "Date Range",
            start = "2015-01-01",
            end = "2021-12-31"
          ),

          actionButton("load_exposure", "Load Data", class = "btn-primary")
        ),

        mainPanel(
          width = 9,

          h4("Daily Time Series"),
          plotlyOutput("exposure_timeseries", height = "400px"),

          hr(),

          fluidRow(
            column(4, h5("Summary Statistics"), verbatimTextOutput("exp_summary")),
            column(4, h5("Temporal Distribution"), plotOutput("exp_calendar")),
            column(4, h5("Value Distribution"), plotOutput("exp_histogram"))
          )
        )
      )
    ),

    # Tab 3: Cohort Summary
    tabPanel(
      "Cohort Summary",

      fluidRow(
        column(
          4,
          h4("Person-Months"),
          plotOutput("cohort_timeline", height = "300px")
        ),
        column(
          4,
          h4("Disease Prevalence"),
          plotOutput("disease_prev", height = "300px")
        ),
        column(
          4,
          h4("Geographic Coverage"),
          plotOutput("geo_coverage", height = "300px")
        )
      ),

      hr(),

      h4("Summary Statistics"),
      verbatimTextOutput("cohort_stats")
    )
  )
)

# Server
server <- function(input, output, session) {

  # Reactive: Load available options
  observe({
    # Get unique model specs
    model_specs <- exwas_ds %>%
      distinct(model_spec_id) %>%
      collect() %>%
      pull(model_spec_id)

    updateSelectInput(session, "model_spec", choices = model_specs, selected = model_specs[1])

    # Get unique outcomes
    outcomes <- exwas_ds %>%
      distinct(outcome) %>%
      collect() %>%
      pull(outcome)

    updateSelectInput(session, "outcome", choices = c("All" = "all", outcomes), selected = "all")

    # Get unique exposures
    exposures <- exp_ds %>%
      distinct(exposure_id) %>%
      collect() %>%
      pull(exposure_id)

    updateSelectInput(session, "exposure_id", choices = exposures, selected = exposures[1])
  })

  # Reactive: Filtered ExWAS results
  filtered_results <- reactive({
    req(input$model_spec)

    ds <- exwas_ds %>%
      filter(
        model_spec_id == input$model_spec,
        q_value <= input$qmax,
        p_value <= input$pmax
      )

    if (input$outcome != "all") {
      ds <- ds %>% filter(outcome == input$outcome)
    }

    collect(ds) %>%
      mutate(
        log10_p = -log10(p_value),
        significant = q_value < 0.05,
        label = paste0(exposure_id, " → ", outcome)
      )
  })

  # Volcano plot
  output$volcano_plot <- renderPlotly({
    df <- filtered_results()

    if (nrow(df) == 0) {
      return(plotly_empty())
    }

    # Identify top hits for labeling
    if (input$show_labels) {
      top_hits <- df %>%
        arrange(p_value) %>%
        head(input$label_top_n)
    } else {
      top_hits <- df %>% filter(FALSE)  # empty
    }

    p <- ggplot(df, aes(x = estimate, y = log10_p, color = significant, text = label)) +
      geom_point(alpha = 0.6, size = 2) +
      geom_hline(yintercept = -log10(0.05), linetype = "dashed", color = "red") +
      geom_vline(xintercept = 0, linetype = "solid", color = "gray50") +
      scale_color_manual(values = c("FALSE" = "gray60", "TRUE" = "#E41A1C")) +
      labs(
        x = "Log Odds Ratio",
        y = "-log10(p-value)",
        color = "FDR < 0.05"
      ) +
      theme_minimal() +
      theme(legend.position = "bottom")

    if (nrow(top_hits) > 0) {
      p <- p + ggrepel::geom_text_repel(
        data = top_hits,
        aes(label = exposure_id),
        size = 3,
        max.overlaps = 20
      )
    }

    ggplotly(p, tooltip = "text")
  })

  # Effect size distribution
  output$effect_dist <- renderPlotly({
    df <- filtered_results()

    if (nrow(df) == 0) {
      return(plotly_empty())
    }

    p <- ggplot(df, aes(x = or, fill = significant)) +
      geom_histogram(bins = 30, alpha = 0.7) +
      geom_vline(xintercept = 1, linetype = "dashed", color = "black") +
      scale_fill_manual(values = c("FALSE" = "gray60", "TRUE" = "#E41A1C")) +
      scale_x_log10() +
      labs(
        x = "Odds Ratio (log scale)",
        y = "Count",
        fill = "FDR < 0.05"
      ) +
      theme_minimal() +
      theme(legend.position = "bottom")

    ggplotly(p)
  })

  # Results table
  output$results_table <- renderDT({
    df <- filtered_results() %>%
      arrange(p_value) %>%
      select(outcome, exposure_id, n, n_cases, or, or_ci_low, or_ci_high, p_value, q_value) %>%
      mutate(
        or = round(or, 3),
        or_ci_low = round(or_ci_low, 3),
        or_ci_high = round(or_ci_high, 3),
        p_value = formatC(p_value, format = "e", digits = 2),
        q_value = round(q_value, 4)
      )

    datatable(
      df,
      options = list(pageLength = 25, scrollX = TRUE),
      filter = "top",
      rownames = FALSE
    )
  })

  # Download handler
  output$download_results <- downloadHandler(
    filename = function() {
      paste0("exwas_results_", Sys.Date(), ".csv")
    },
    content = function(file) {
      write.csv(filtered_results(), file, row.names = FALSE)
    }
  )

  # Exposure time series
  exposure_data <- eventReactive(input$load_exposure, {
    req(input$geo_id, input$exposure_id)

    exp_ds %>%
      filter(
        geo_id == input$geo_id,
        geo_type == input$geo_type,
        exposure_id == input$exposure_id,
        obs_date >= as.Date(input$date_range[1]),
        obs_date <= as.Date(input$date_range[2])
      ) %>%
      collect() %>%
      arrange(obs_date)
  })

  output$exposure_timeseries <- renderPlotly({
    df <- exposure_data()

    if (nrow(df) == 0) {
      return(plotly_empty())
    }

    p <- ggplot(df, aes(x = obs_date, y = value)) +
      geom_line(color = "#377EB8") +
      labs(x = "Date", y = "Exposure Value", title = input$exposure_id) +
      theme_minimal()

    ggplotly(p)
  })

  output$exp_summary <- renderPrint({
    df <- exposure_data()
    if (nrow(df) == 0) return("No data")

    cat("Mean:", round(mean(df$value, na.rm = TRUE), 2), "\n")
    cat("Median:", round(median(df$value, na.rm = TRUE), 2), "\n")
    cat("SD:", round(sd(df$value, na.rm = TRUE), 2), "\n")
    cat("Min:", round(min(df$value, na.rm = TRUE), 2), "\n")
    cat("Max:", round(max(df$value, na.rm = TRUE), 2), "\n")
    cat("Missing:", sum(is.na(df$value)), "days\n")
  })

  # Cohort summary stats
  output$cohort_stats <- renderPrint({
    pm <- pm_ds %>% collect()

    cat("=== Cohort Overview ===\n\n")
    cat("Unique persons:", comma(n_distinct(pm$person_id)), "\n")
    cat("Person-months:", comma(nrow(pm)), "\n")
    cat("Date range:", min(pm$ym), "to", max(pm$ym), "\n")
    cat("\nGeographic coverage:\n")
    cat("  With tract:", comma(sum(!is.na(pm$tract_geoid))),
        paste0("(", round(100 * mean(!is.na(pm$tract_geoid)), 1), "%)\n"))
    cat("  With ZIP:", comma(sum(!is.na(pm$zip5))),
        paste0("(", round(100 * mean(!is.na(pm$zip5)), 1), "%)\n"))
  })
}

shinyApp(ui, server)