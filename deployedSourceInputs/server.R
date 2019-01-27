library(rworldmap)
shinyServer(function(input, output, session) {
  # Send a pre-rendered image, and don't delete the image after sending it
  output $ preImage_horiz <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/picture',
                          paste('horiz_plot_2016_', input$month, '.png', sep=''))
    
    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$month))
    
  }, deleteFile = FALSE)
  # --------
  # image2 sends pre-rendered images
  output $ preImage_pie <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/picture',
                          paste('hour_pie_2016_', input$pie_month, '.png', sep=''))
    
    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$pie_month))
    
  }, deleteFile = FALSE)
  
  # image2 sends pre-rendered images
  output $ preImage_per_hour <- renderImage({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/picture',
                          paste('hour_transaction_2016_', input$per_hour_month, '.png', sep=''))
    
    # Return a list containing the filename and alt text
    list(src = filename,
         alt = paste("Image number", input$per_hour_month))
    
  }, deleteFile = FALSE)
  
  # render rworld map according to country
  output $ Image_geo_distribution <- renderPlot({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/csv',
                          paste('fixed_geo_only_list_2016_', input$geo_per_month, '.csv', sep=''))
    dF = read.csv(filename, sep = ',', header = TRUE)
    sPDF <- joinCountryData2Map(dF,
                                joinCode = "NAME",
                                nameJoinColumn = "Country",
                                verbose = TRUE)
    
    library(RColorBrewer)
    # colourPalette <- brewer.pal(9,'PuBuGn')
    colourPalette <- brewer.pal(9,'PuBuGn')
    mapCountryData(sPDF, nameColumnToPlot="transaction",colourPalette=colourPalette,catMethod='fixedWidth')
  }, width = 1200, height = 1000)
  
  
  output $ Image_geo_barplot <- renderPlot({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/csv',
                          paste('fixed_geo_only_list_2016_', input$geo_per_month_bar, '.csv', sep=''))
    dF = read.csv(filename, sep = ',', header = TRUE)
    sPDF <- joinCountryData2Map(dF,
                                joinCode = "NAME",
                                nameJoinColumn = "Country",
                                verbose = TRUE)
    
    library(RColorBrewer)
    # colourPalette <- brewer.pal(9,'PuBuGn')
    colourPalette <- brewer.pal(9,'PuBuGn')
    barplotCountryData(sPDF,nameColumnToPlot="transaction",nameCountryColumn = "Country",numCats = 10,colourPalette = colourPalette, na.last = NA, decreasing = TRUE, scaleSameInPanels = FALSE, numPanels = 5, cex = 1.1)
  
    }, width = 1500, height = 1000)
  
  
  output $ Image_geo_pieplot <- renderPlot({
    # When input$n is 3, filename is ./images/image3.jpeg
    filename <- file.path('/Users/user/song_user_preference/csv',
                          paste('fixed_geo_only_list_2016_', input$geo_per_month_pie, '.csv', sep=''))
    dF = read.csv(filename, sep = ',', header = TRUE)
    library(plotrix)
    slices_raw <- dF[[2]]
    lbls_raw <- dF[[1]]
    sample_num <- 15
    slices <- slices_raw[1:sample_num]
    lbls <- lbls_raw[1:sample_num]
    
    n <- 30
    
    pct <- round(slices/sum(slices)*100)
    
    lbls <- paste(lbls, pct) # add percents to labels 
    lbls <- paste(lbls,"%",sep="") # ad % to labels 
    pie3D(slices,labels=lbls,explode=0.1, radius=0.9,col=rainbow(length(lbls)), labelrad = 3, minsep=0.5,main="Pie Chart of Countries ", theta=pi/8,start = 0,border=par("fg"),labelcol=par("fg"),labelcex=1.5)
    #pie(slices,labels = lbls,
    #    main="Pie Chart of Countries")
  }, width = 1200, height = 1000)
  
})
