import duckdb
import pandas as pd
import wordcloud

# 引入停止词模块

from wordcloud import WordCloud, STOPWORDS

import lib.words

duckdb.sql(
    """
with
    exclude_words as (
                     select distinct Remark as word from read_csv('/Users/hcm-b0494/Desktop/chats/group_chats.csv')
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
               from chats.main.wx_chats
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

# 读取 CSV 文件到 DataFrame
df = pd.read_csv('./tmp/temp-1.csv')

# 过滤数据
charts = df
charts["StrContent"] = charts["StrContent"].fillna("").astype(str)

stopwords = STOPWORDS
# 添加新的停止词
stopwords.update(
    lib.words.filter_words)

wc = wordcloud.WordCloud(
    font_path="./lib/SIMSUN.ttf",
    width=2000,
    height=2000,
    background_color="white",
    max_words=250,
    # 配置停止词参数
    stopwords=stopwords)

text = " ".join(
    charts["StrContent"])
wc.generate(text)  # 生成词云
wc.to_file("./result/resultStop.png")
