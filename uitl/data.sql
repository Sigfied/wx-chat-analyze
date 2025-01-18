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
               from read_csv('./lib/merged_group_chat_rename.csv')
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
                     from read_csv('./lib/merged_group_chat_rename.csv')
                     where Type not in (
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
;




-- 撤回消息指标
select REGEXP_EXTRACT(StrContent, '"([^"]+)"') AS name,          -- 提取名字
       COUNT(*)                                AS withdraw_count -- 统计撤回次数
from read_csv('./lib/group_chats.csv')
where Type = 10000
GROUP BY name -- 按名字分组
ORDER BY withdraw_count DESC;
-- 按撤回次数降序排序




