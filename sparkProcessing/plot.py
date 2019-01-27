import numpy as np
import pandas as pd
import io
import matplotlib.pyplot as plt
import seaborn as sns
hour_count = []
df = pd.read_csv('save_csv.csv', delimiter=',',names= ['day','hour','count'],header = 0, index_col = 'day')

for key, item in df.groupby('hour'):
    groupby_hour = df.groupby('hour').get_group(key)
    transaction_by_hour = groupby_hour['count'].sum()
    hour_count.append((key, transaction_by_hour))
    # print len(transaction_by_hour) 
    
ndays = len(np.unique(df.index.values))
df_hour_count = pd.DataFrame(hour_count)

# for horizontal bar plot
df.pivot(columns='hour',values='count').plot.barh(stacked=True, figsize=(30, 20))
# ------ bar chart --------
df.series.plot.pie(figsize=(6, 6))


# for one specific hour, all month transaction summation.
df_hour_count.plot.bar(figsize=(10, 6), legend=False, color = 'blue')
# df.pivot(columns='hour',values='count').plot.area()
plt.show()