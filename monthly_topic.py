from collections import defaultdict, Counter

import duckdb
import jieba
import pandas as pd

from lib.words import filter_words

duckdb.sql(
    """
with
    exclude_words as (
                     select distinct Remark as word from read_csv('./lib/group_chats.csv')
                     )
  , chats as (
              select TalkerId,
                      Type,
                      StrContent,
                      StrTime,
                      Remark,
                      CASE
                          WHEN cast(StrTime as TIME) BETWEEN '00:00:00' AND '06:00:00'
                              THEN cast(StrTime as timestamp) - INTERVAL '1' DAY
                          ELSE cast(StrTime as timestamp)
                          END
                          AS adjusted_timestamp,
               from read_csv('./lib/group_chats.csv')
               where Type = 1
                     )

select *
from
    (
    select *
    from
        chats
    where
          not exists
              (
              select 1 from exclude_words where chats.StrContent like '%' || exclude_words.word || '%'
              )
    )
    """
).to_csv('./tmp/temp-1.csv', encoding='utf-8')


# 读取CSV文件
df = pd.read_csv('./tmp/temp-1.csv')
# for word in filter_words:
#     df['StrContent'] = df['StrContent'].str.replace(f'[{word}]', '', regex=True)

df['StrContent'] = df['StrContent'].str.replace(r'[0-9a-zA-Z]', '', regex=True)

# 将StrTime列转换为datetime类型
df['StrTime'] = pd.to_datetime(df['StrTime'])

# 提取月份
df['Month'] = df['StrTime'].dt.to_period('M')

# 按月份分组
grouped = df.groupby('Month')


# 定义一个函数，用于分词并统计词频
def get_keywords(texts, top_n=10):
    # 合并所有文本
    combined_text = " ".join(texts)
    # 使用 jieba 分词
    words = jieba.lcut(combined_text)
    # 过滤掉停用词和单字词，并控制词语长度
    min_length = 3  # 最小长度
    max_length = 7  # 最大长度
    # 过滤掉停用词和单字词
    words = [w for w in words if min_length <= len(w) <= max_length and w not in filter_words]
    # 统计词频
    word_counts = Counter(words)
    # 返回词频最高的 top_n 个词
    return word_counts.most_common(top_n)


# 统计每个月的关键词
monthly_keywords = {}
for month, group in grouped:
    texts = group['StrContent'].tolist()
    keywords = get_keywords(texts, top_n=15)  # 提取前5个关键词
    monthly_keywords[month] = keywords

(pd.DataFrame(monthly_keywords)
 .transpose()
 .to_csv('./result/monthly_keywords.csv', encoding='utf-8'))

# 输出结果
for month, keywords in monthly_keywords.items():
    print(f"月份: {month}, {len(keywords)}")
    for word, freq in keywords:
        print(f"关键词: {word}, 词频: {freq}")
    print("------")

