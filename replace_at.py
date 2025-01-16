import re

import pandas as pd

# 读取 CSV 文件
file_path = './lib/group_chats.csv'
df = pd.read_csv(file_path)


# 定义替换函数
def remove_at_content(text, remarks):
    """
    删除 @ 开头并包含 remarks 中任意一个值的部分
    """

    # 遍历 remarks 数组
    for remark in remarks:
        if pd.isna(remark):  # 如果 remark 为空，跳过
            continue
        # 构造匹配模式：@ + remark + 8 个字符
        pattern = f"@{remark}.{{8}}"  # .{8} 表示匹配任意 8 个字符
        # 使用正则表达式替换
        text = re.sub(pattern, '', text)
    return text


# 应用替换函数
df['StrContent'] = df.apply(lambda row: remove_at_content(row['StrContent'], remarks=[

]), axis=1)

# 保存结果（可选）
output_path = './result/group_chats_cleaned.csv'
df.to_csv(output_path, index=False, encoding='utf-8')

# 打印处理后的数据
print("\n处理后的数据：")
print(df.head())
