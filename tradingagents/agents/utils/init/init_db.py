import sqlite3

# 数据库实例对象 conn 用于创建数据库连接，cursor 用于执行 SQL 语句
conn = sqlite3.connect("memory.db")
cursor = conn.cursor()