import numpy as np
import pandas as pd
import io
import matplotlib.pyplot as plt
import seaborn as sns
import sys
hour_count = []
month = str(sys.argv[1])
raw_csv = pd.read_csv('/Users/user/song_user_preference/csv/fixed_geo_only_list_'+ month +'.csv', delimiter=',',names= ['Country','transaction'],header = 0)
ndays = len(np.unique(df.index.values))

# for key, item in df.groupby('hour'):
#     groupby_hour = df.groupby('hour').get_group(key)
# #     series = pd.Series(groupby_hour['count'], name='Component')
# #     series.plot.pie(figsize=(10, 10), labels=list(range(ndays+1)),autopct='%.2f', fontsize=10)
# #     plt.show()
#     transaction_by_hour = groupby_hour['count'].sum()
#     hour_count.append((key, transaction_by_hour))
#     # print len(transaction_by_hour) 


# print(hour_count)

df = pd.DataFrame(raw_csv)
print df

# for horizontal bar plot
# df.pivot(columns='hour',values='count').plot.barh(stacked=True, figsize=(15, 10))
# plt.savefig('horiz_plot_' + month + '.png')
# plt.clf()
# df_hour_count.plot.bar(figsize=(10, 6), legend=False, color = 'blue')
# plt.savefig('hour_transaction_' + month + '.png')
# plt.clf()
comp = pd.Series(df[:2], name='Pie')
print comp
comp.plot.pie(figsize=(10, 10), labels=list(range(24)),autopct='%.2f', fontsize=10)
plt.savefig('geo_pie_' + month + '.png')
plt.clf()

print 'End'