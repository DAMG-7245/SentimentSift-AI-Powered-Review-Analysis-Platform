import pandas as pd
import numpy as np
import os
import sys
import json
import snowflake.connector
from typing import Dict, List, Any
from dotenv import load_dotenv

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    从配置文件或环境变量加载Snowflake配置
    
    Args:
        config_path: 配置文件路径（可选）
        
    Returns:
        包含Snowflake配置的字典
    """
    # 优先从环境变量加载
    load_dotenv()
    
    # 检查环境变量是否存在
    if all(key in os.environ for key in [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]):
        return {
            'account': os.environ['SNOWFLAKE_ACCOUNT'],
            'user': os.environ['SNOWFLAKE_USER'],
            'password': os.environ['SNOWFLAKE_PASSWORD'],
            'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
            'database': os.environ['SNOWFLAKE_DATABASE'],
            'schema': os.environ['SNOWFLAKE_SCHEMA']
        }
    
    # 如果环境变量不完整且提供了配置文件路径，从配置文件加载
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    
    # 如果环境变量和配置文件都不可用，抛出异常
    raise ValueError("无法加载Snowflake配置。请确保.env文件或配置文件存在并包含所需信息。")

def json_to_dataframe(json_file_path: str) -> pd.DataFrame:
    """
    将JSON文件转换为DataFrame，包含所有主题信息
    
    Args:
        json_file_path: JSON文件路径
        
    Returns:
        包含数据的DataFrame
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    all_records = []
    
    for business in data:
        # 提取基本商家信息
        base_record = {k: v for k, v in business.items() if not isinstance(v, (dict, list))}
        
        # 处理情感分析分数
        if 'sentiment_scores' in business:
            for key, value in business['sentiment_scores'].items():
                base_record[f'sentiment_{key}'] = value
        
        # 处理主题数据
        topics = business.get('topics', [])
        if not topics:
            # 如果没有主题数据，仍添加一条记录
            all_records.append(base_record)
        else:
            # 为每个主题创建完整记录
            for topic_idx, topic in enumerate(topics):
                # 复制基本记录
                record = base_record.copy()
                
                # 添加主题数据
                try:
                    record['topic_id'] = topic['topic_id']
                    record['topic_count'] = int(topic['count']) if isinstance(topic['count'], str) else topic['count']
                    record['topic_keywords'] = topic['keywords']
                    record['topic_name'] = topic['name']
                    record['topic_rank'] = topic_idx + 1  # 主题排名
                    
                    # 添加完整记录
                    all_records.append(record)
                except Exception as e:
                    print(f"处理主题数据时出错: {str(e)}")
                    print(f"问题数据: {topic}")
    
    return pd.DataFrame(all_records)

def create_snowflake_table(conn, table_name: str, columns: Dict[str, str]):
    """
    在Snowflake中创建表
    
    Args:
        conn: Snowflake连接对象
        table_name: 表名
        columns: 列名到数据类型的映射
    """
    cursor = conn.cursor()
    try:
        column_defs = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
        
        print(f"创建表 {table_name}")
        cursor.execute(query)
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        print(f"查询: {query}")
        raise
    finally:
        cursor.close()

def insert_dataframe_to_snowflake(conn, table_name: str, df: pd.DataFrame):
    """
    将DataFrame插入到Snowflake表中
    
    Args:
        conn: Snowflake连接对象
        table_name: 表名
        df: 要插入的DataFrame
    """
    # 如果DataFrame为空，直接返回
    if df.empty:
        print(f"提供的DataFrame为空，跳过插入表 {table_name}")
        return
    
    # 处理NaN值，将其替换为None (会被转换为SQL中的NULL)
    df_clean = df.copy()
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].replace({np.nan: None})
    
    # 转换DataFrame行为元组列表
    values = [tuple(row) for row in df_clean.values]
    
    # 生成INSERT语句的占位符
    placeholders = ", ".join(["(%s)" % ", ".join(["%s"] * len(df_clean.columns))])
    
    # 生成列名
    column_names = ", ".join(df_clean.columns)
    
    # 生成INSERT语句
    query = f'INSERT INTO {table_name} ({column_names}) VALUES {placeholders}'
    
    print(f"向表 {table_name} 插入 {len(df_clean)} 条记录")
    cursor = conn.cursor()
    try:
        cursor.executemany(query, values)
    except Exception as e:
        print(f"插入数据时出错: {str(e)}")
        print(f"查询: {query[0:500]}...")  # 只打印前500个字符以避免过长
        print(f"数据框列: {df_clean.columns.tolist()}")
        print(f"第一行样本: {values[0] if values else '无数据'}")
        raise
    finally:
        cursor.close()

def clear_table(conn, table_name: str):
    """
    清空表中的所有数据
    
    Args:
        conn: Snowflake连接对象
        table_name: 表名
    """
    cursor = conn.cursor()
    try:
        query = f"DELETE FROM {table_name}"
        print(f"清空表 {table_name}")
        cursor.execute(query)
    except Exception as e:
        print(f"清空表时出错: {str(e)}")
        raise
    finally:
        cursor.close()

def sync_to_single_table(conn, integrated_file_path: str, table_name: str = "COFFEE_SHOPS"):
    """
    将整合的JSON数据同步到单个Snowflake表
    
    Args:
        conn: Snowflake连接对象
        integrated_file_path: 整合JSON文件的路径
        table_name: 目标表名
    """
    # 将JSON转换为DataFrame
    df = json_to_dataframe(integrated_file_path)
    
    # 处理可能存在的列表值
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    
    # 列名转换为大写
    df.columns = [col.upper() for col in df.columns]
    
    # 生成schema字典
    columns = {}
    for col in df.columns:
        dtype = df[col].dtype
        
        if col == 'BUSINESS_ID':
            columns[col] = 'VARCHAR(255)'
        elif 'ID' in col:
            columns[col] = 'VARCHAR(255)'
        elif dtype == 'object':
            max_len = df[col].astype(str).str.len().max()
            if max_len <= 255:
                columns[col] = 'VARCHAR(255)'
            elif max_len <= 1000:
                columns[col] = 'VARCHAR(1000)'
            else:
                columns[col] = 'TEXT'
        elif pd.api.types.is_float_dtype(dtype):
            columns[col] = 'FLOAT'
        elif pd.api.types.is_integer_dtype(dtype):
            columns[col] = 'INTEGER'
        else:
            columns[col] = 'VARCHAR(255)'
    
    # 添加复合主键（如果需要）
    # 注意: 这将创建一个联合主键 (business_id, topic_rank)
    # columns["PRIMARY KEY"] = "(BUSINESS_ID, TOPIC_RANK)"
    
    # 创建表
    create_snowflake_table(conn, table_name, columns)
    
    # 清空表，然后插入所有记录
    clear_table(conn, table_name)
    insert_dataframe_to_snowflake(conn, table_name, df)
    
    print(f"成功同步 {len(df)} 条记录到表 {table_name}")

def run_snowflake_sync(config_path: str = None, integrated_business_path: str = None):
    """
    运行Snowflake同步过程
    
    Args:
        config_path: Snowflake配置文件路径（如果使用环境变量则可选）
        integrated_business_path: 整合JSON文件的路径
    """
    # 设置默认路径
    if integrated_business_path is None:
        integrated_business_path = "data/merge/integrated_business.json"
    
    # 加载Snowflake配置
    config = load_config(config_path)
    
    # 连接到Snowflake
    conn = None
    try:
        print("连接到Snowflake...")
        conn = snowflake.connector.connect(
            account=config['account'],
            user=config['user'],
            password=config['password'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema']
        )
        
        # 同步到单个表
        sync_to_single_table(conn, integrated_business_path)
        
        print("Snowflake 同步完成")
        
    except Exception as e:
        print(f"同步过程中发生错误: {str(e)}")
    finally:
        # 断开Snowflake连接
        if conn:
            conn.close()
            print("Snowflake连接已关闭")

if __name__ == "__main__":
    # 默认情况下从环境变量加载配置
    # 可以通过参数指定配置文件
    config_path = None
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    # 默认整合数据文件路径
    integrated_business_path = "data/merge/integrated_business.json"
    if len(sys.argv) > 2:
        integrated_business_path = sys.argv[2]
    
    run_snowflake_sync(config_path, integrated_business_path)