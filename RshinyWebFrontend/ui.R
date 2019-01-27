library(shiny)

# Define UI for random distribution application 
fluidPage(
  
  # Application title
  titlePanel("Tabsets"),
  
  # Sidebar with controls to select the random distribution type
  # and number of observations to generate. Note the use of the
  # br() element to introduce extra vertical spacing
  sidebarLayout(
    sidebarPanel(
      selectInput("month",
                  "Total transaction count(month): ",
                  c("01","02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")), 
      br(),
      selectInput("pie_month",
                  "Pie Month:",
                  c("01","02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")),
      br(),
      selectInput("per_hour_month",
                  "Total transaction per hour: ",
                  c("01","02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")), 
      br(),
      
      sliderInput("n", 
                  "Number of observations:", 
                  value = 500,
                  min = 1, 
                  max = 1000)
    ),
    
    # Show a tabset that includes a plot, summary, and table view
    # of the generated distribution
    mainPanel(
      tabsetPanel(type = "tabs", 
                  tabPanel("stack plot", imageOutput("preImage_horiz")), 
                  tabPanel("Pie plot", imageOutput("preImage_pie")), 
                  tabPanel("Componnent -- every hour", imageOutput("preImage_per_hour")), 
                  tabPanel("Table", tableOutput("table"))
      )
    )
  )
)