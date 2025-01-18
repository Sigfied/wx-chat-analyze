from pathlib import Path

import duckdb
import pandas as pd

name_map = {

}


def wx_report(chats_csv_file='./lib/merged_group_chat.csv'):
    tmp = './tmp'
    tmp_path = Path(tmp)
    if not tmp_path.exists():
        # 创建路径（包括所有中间目录）
        tmp_path.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        f"""
    -- 最早的聊天，以及聊天正文
with chats as (select 
                      StrContent,
                      StrTime,
                      Remark,
                      Type as MsgType,
                      CASE
                          WHEN cast(StrTime as TIME) BETWEEN '00:00:00' AND '06:00:00'
                              THEN cast(StrTime as timestamp) - INTERVAL '1' DAY
                          ELSE cast(StrTime as timestamp)
                          END
                          AS adjusted_timestamp
               from read_csv('{chats_csv_file}')
               where Remark is not null and Remark <> ''
            )

   , min_chat_time as (select Remark,
                              min(StrTime)                                   as MinStrTime,
                              count(*)                                       as chat_nums,
                              DATEDIFF('day', MinStrTime, CURRENT_TIMESTAMP) AS days_diff
                       from chats
                       where MsgType = 1
                       group by Remark)

   , min_chat_content as (
                          select Remark,EarliestContent,MinStrTime,chat_nums,days_diff
                            from (
                            select min_chat_time.Remark,
                                 chats.StrContent as EarliestContent,
                                 min_chat_time.MinStrTime,
                                 min_chat_time.chat_nums,
                                 min_chat_time.days_diff,
                                 ROW_NUMBER() OVER (PARTITION BY min_chat_time.Remark ORDER BY chats.StrContent DESC) AS rank
                          from min_chat_time
                                   left join chats on min_chat_time.Remark = chats.Remark and chats.MsgType = 1
                              and min_chat_time.MinStrTime = chats.StrTime)
                            where rank = 1
                              )
                              

   , sum_chat_date as (select Remark, max_chat_date, max_chat_date_chats
                       from (SELECT Remark,
                                    STRFTIME(StrTime, '%Y年%m月%d日') AS max_chat_date,
                                    COUNT(*)                          AS max_chat_date_chats,
                                    ROW_NUMBER()                         OVER (PARTITION BY Remark ORDER BY COUNT(*) DESC) AS rank  -- 按 max_chat_date_chats 降序排序
                             FROM chats
                             GROUP BY Remark, max_chat_date)
                       where rank = 1)

   , latest_of_day_chats as (select Remark, latest_of_day_time
                             from (SELECT Remark,

                                          STRFTIME(adjusted_timestamp, '%H:%M:%S') AS latest_of_day_time,
                                          ROW_NUMBER()                                OVER (PARTITION BY Remark ORDER BY STRFTIME(adjusted_timestamp, '%H:%M:%S') ) AS rank  -- 按 max_chat_date_chats 降序排序
                                   FROM chats
                                   GROUP BY Remark, latest_of_day_time)
                             where rank = 1)

   , share_chats as (select Remark, count(*) as share_counts
                     from chats
                     where MsgType not in (
                                        10000, 49, 1
                         )
                     group by Remark)


select min_chat_content.Remark,
       min_chat_content.EarliestContent,
       min_chat_content.MinStrTime,
       min_chat_content.chat_nums,
       min_chat_content.days_diff,
       min_chat_content.chat_nums / min_chat_content.days_diff as dayliy_chats,
       -- 最多聊天
       sum_chat_date.max_chat_date,
       sum_chat_date.max_chat_date_chats,
       latest_of_day_chats.latest_of_day_time,
       share_chats.share_counts
from min_chat_content
         left join sum_chat_date
                   on min_chat_content.Remark = sum_chat_date.Remark
         left join latest_of_day_chats
                   on min_chat_content.Remark = latest_of_day_chats.Remark

         left join share_chats on min_chat_content.Remark = share_chats.Remark
    """
    ).to_csv(tmp + '/temp-2.csv', encoding='utf-8')
    return pd.read_csv(tmp + '/temp-2.csv')


def wx_remarks(chats_csv_file='./lib/merged_group_chat.csv'):
    return duckdb.sql(f"""
     select 
        *
   from read_csv('{chats_csv_file}')
    """).df()


df = wx_report('./lib/merged_group_chat_rename.csv')
print(df)
df.to_csv('./result/report.csv', encoding='utf-8')
