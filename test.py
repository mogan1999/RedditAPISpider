import json

# 假设你有一个 JSON 数据：
data = {
    "name": "张三",
    "age": 30,
    "city": "北京"
}

# 使用 json.dumps() 方法美化 JSON 数据：
pretty_data = json.dumps(data, indent=4, ensure_ascii=False)

# 输出美化后的 JSON 数据：
print(pretty_data)
