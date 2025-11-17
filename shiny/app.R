library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)

root <- Sys.getenv("PARQUET_ROOT", unset = "data")

exwas_ds <- arrow::open_dataset(
  fs::path(root, "gold", "exwas_result")
)

ui <- fluidPage(
  titlePanel("PEGS Explorer — Parquet MVP"),
  sidebarLayout(
    sidebarPanel(
      textInput(
        "model",
        "Model Spec ID (optional)",
        ""
      ),
      sliderInput(
        "qmax",
        "Max q-value",
        min   = 0,
        max   = 1,
        value = 0.1,
        step  = 0.01
      )
    ),
    mainPanel(
      plotOutput("volcano"),
      tableOutput("res")
    )
  )
)

server <- function(input, output, session) {

  res <- reactive({
    ds <- exwas_ds %>%
      dplyr::filter(q_value <= input$qmax)

    if (nzchar(input$model)) {
      ds <- ds %>%
        dplyr::filter(model_spec_id == input$model)
    }

    dplyr::collect(ds)
  })

  output$volcano <- renderPlot({
    df <- res()

    if (!nrow(df)) {
      return(NULL)
    }

    ggplot(df, aes(x = estimate, y = -log10(p_value))) +
      geom_point() +
      geom_hline(yintercept = -log10(0.05), linetype = 2) +
      theme_minimal()
  })

  output$res <- renderTable({
    res()
  })
}

shinyApp(ui, server)
