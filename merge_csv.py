import pandas as pd

# 读取两个 CSV 文件
df1 = pd.read_csv('./lib/group_chat_1.csv')
df2 = pd.read_csv('./lib/group_chats.csv')

# 假设时间列的名称为 'timestamp'，将其转换为 datetime 类型
df1['d_timestamp'] = pd.to_datetime(df1['StrTime'])
df2['d_timestamp'] = pd.to_datetime(df2['StrTime'])

# 定义时间条件
cutoff_time = pd.to_datetime('2024-11-01 00:33:30')

# 筛选 group_chat_1 中大于 cutoff_time 的数据
df1_filtered = df1[df1['d_timestamp'] > cutoff_time]

# 筛选 group_chat_2 中小于等于 cutoff_time 的数据
df2_filtered = df2[df2['d_timestamp'] <= cutoff_time]

# 合并两个数据集
merged_df = pd.concat([df1_filtered, df2_filtered])

# 保存合并后的数据到新的 CSV 文件
merged_df.to_csv('./lib/merged_group_chat.csv', index=False)

print("合并完成，结果已保存到 'merged_group_chat.csv'")