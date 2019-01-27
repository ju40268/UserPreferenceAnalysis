shinyServer(function(input, output, session) {
  # Send a pre-rendered image, and don't delete the image after sending it
  output $ preImage_horiz <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user',
                                        paste('horiz_plot_2016_', input$month, '.png', sep=''))

    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$month))
    
  }, deleteFile = FALSE)
  # --------
  # image2 sends pre-rendered images
  output $ preImage_pie <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user',
                          paste('hour_pie_2016_', input$pie_month, '.png', sep=''))
    
    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$pie_month))
    
  }, deleteFile = FALSE)
  
  # image2 sends pre-rendered images
  output $ preImage_per_hour <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user',
                          paste('hour_transaction_2016_', input$per_hour_month, '.png', sep=''))
    
    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$per_hour_month))
    
  }, deleteFile = FALSE)
  
})

#function(input, output) {
  
  # You can access the value of the widget with input$select, e.g.
#  output$value <- renderPrint({ input$select })
#  output$preImage
#}