from collections import defaultdict, Counter
from pathlib import Path
from typing import Literal

import duckdb
import jieba
import pandas as pd

period_mapping = {
    'Month': 'M',
    'Year': 'Y',
    'Day': 'D',
    'Quarter': 'Q',  # 季度
    'Week': 'W'  # 周
}

type AggregationModel = Literal['Month', 'Year', 'Day']


def get_chats(csv_file_path):
    tmp_path = Path('./tmp/temp-1.csv')
    if not tmp_path.exists():
        # 创建路径（包括所有中间目录）
        tmp_path.mkdir(parents=True, exist_ok=True)

    # noinspection SqlDialectInspection
    duckdb.sql(
        f"""
    with
        exclude_words as (
                         select distinct Remark as word from read_csv('{csv_file_path}')
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
    ).to_csv(tmp_path.name, encoding='utf-8')
    df = pd.read_csv(tmp_path.name)
    return df


def aggregation_chats(df,
                      agg_model: AggregationModel = 'Month'):
    df['StrContent'] = df['StrContent'].str.replace(r'[0-9a-zA-Z]', '', regex=True)

    # 将StrTime列转换为datetime类型
    df['StrTime'] = pd.to_datetime(df['StrTime'])

    # 提取月份
    df[agg_model] = df['StrTime'].dt.to_period(period_mapping[agg_model])

    # 按月份分组
    grouped = df.groupby(agg_model)
    return grouped


# 定义一个函数，用于分词并统计词频
def get_keywords(texts,
                 min_length=3, max_length=7,
                 top_n=10,
                 stopword_file='./lib/stopwords_hit_modified.txt'
                 ):
    try:
        with open(f'{stopword_file}', 'r', encoding='utf-8') as f1:
            stop_words = set(f1.read().splitlines())
    except FileNotFoundError:
        print(f"停顿词文件异常,停顿词使用空数组")
        stop_words = set()

    # 合并所有文本
    combined_text = " ".join(texts)
    # 使用 jieba 分词
    words = jieba.lcut(combined_text)
    # 过滤掉停用词和单字词，并控制词语长度
    # 过滤掉停用词和单字词
    words = [w for w in words if min_length <= len(w) <= max_length and w not in stop_words]
    # 统计词频
    word_counts = Counter(words)
    # 返回词频最高的 top_n 个词
    return word_counts.most_common(top_n)


def hot_topic_top_n(top_n=15, min_length=3, max_length=7, csv_file_path='./lib/merged_group_chat.csv',
                    result_path='./result/monthly_keywords.csv',
                    agg_model: AggregationModel = 'Month',
                    stopword_file='./input/stopwords_hit.txt'
                    ):
    """
    获取年度，月度聊天的关键词(热词)
    :param stopword_file:
    :param top_n: 关键词的个数
    :param min_length: 关键词的最小长度
    :param max_length: 关键词的最大长度
    :param csv_file_path: 聊天记录的 CSV 文件路径
    :param result_path: 生成的结果 CSV 文件路径
    :param agg_model: 聚合条件 AggregationModel
    :return:
    """
    path = Path(result_path)
    # 检查路径是否存在，如果不存在则创建
    if not path.exists():
        # 创建路径（包括所有中间目录）
        path.mkdir(parents=True, exist_ok=True)

    df = get_chats(csv_file_path)
    grouped = aggregation_chats(df, agg_model)

    # 统计每个月的关键词
    monthly_keywords = {}
    for month, group in grouped:
        texts = group['StrContent'].tolist()
        keywords = get_keywords(texts, top_n=top_n, min_length=min_length, max_length=max_length,
                                stopword_file=stopword_file)
        monthly_keywords[month] = keywords

    (pd.DataFrame(monthly_keywords)
     .transpose()
     .to_csv(result_path, encoding='utf-8'))


if __name__ == '__main__':
    hot_topic_top_n(agg_model='Month', csv_file_path='./lib/merged_group_chat.csv',
                    stopword_file='./lib/stopwords_hit_modified.txt')
