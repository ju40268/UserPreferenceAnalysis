# Logitech Internship Project -- Spark Analysis for Device Logs

## Visualization Website : [stats result](https://dlwo-logitech.shinyapps.io/transmit_00/)

## Objective
* Developed user click log recommendation system based on timestamp, countries, and music content.
* Extracted unstructured raw data at terabytes scale and built data pipelines using Pyspark.
* Classified soundtrack into genres and matched features to audio products as recommendation results.
* Visualized requests using React framework with Rshiny and D3.js world map.

## Sample Input
```
[2017-05-01 00:00:01] 00:04:20:2c:d1:cd userid:481693 83.243.128.189 sn_newsong {"url":"http://opml.radiotime.com/Tune.ashx?id=s25537&formats=aac,ogg,mp3,wmpro,wma,wmvoice&partnerId=16&serial=e162495252632f6e8db708607cf8d94a","secs":300} 
```
in the format of [timestamp, mac_addr, user_id, ip_addr, music_url, duration]

## Sample Output
aggregate all countries, (Date, Hour) - Transaction Amount
```
day,hour,value
01,00,195739
```
fixed date: Country - Transaction Amount

```
Country,transaction
Germany,43135121
Italy,33454518
United States of America,28295784
```

## Sample Stats Picture



![alt text](https://github.com/may811204/UserPreferenceAnaylsis/blob/master/statsPlots/horiz_plot_2016_02.png "Hourly Transaction Amount")


