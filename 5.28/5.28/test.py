import requests

#response = requests.get('http://localhost:5000/movies')
#print(response.json())#选择全部电影列表

# 按评分筛选电影，添加 min_rating 参数
params = {'min_rating': 8.0}  # 这里可以根据需要修改评分值
response = requests.get('http://localhost:5000/movies/filter', params=params)
print(response.json())