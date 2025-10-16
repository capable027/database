# encoding: utf-8

import os

import matplotlib.pyplot as plt
import pandas as pd
import requests  # 网络请求模块
from lxml import etree  # 数据解析模块
from wordcloud import WordCloud

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False


class DoubanSpider:
    def __init__(self):
        self.urls = ['https://movie.douban.com/top250?start={}&filter='.format(str(i * 25)) for i in range(10)]
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.46'
        }
        self.file_name = "豆瓣电影top250数据"

    def get_first_text(self, list, index=0):
        try:
            return list[index].strip()  # 返回第一个字符串，除去两端的空格
        except:
            return ""  # 返回空字符串

    def spiltCountry(self, str):
        year = str.split("/")[0].strip()  # 年份
        country = str.split("/")[1].strip()  # 国家
        type = str.split("/")[2].strip()  # 类型
        return year, country, type

    def run(self):
        data_list = []  # 存储所有解析后的数据
        count = 1  # 用来计数

        for url in self.urls:
            res = requests.get(url=url, headers=self.headers)  # 发起请求
            res.encoding = res.apparent_encoding  # 解决乱码问题
            html = etree.HTML(res.text)  # 将返回的文本加工为可以解析的html
            lis = html.xpath('//*[@id="content"]/div/div[1]/ol/li')  # 获取每个电影的li元素

            # 解析数据
            for li in lis:
                title = self.get_first_text(li.xpath('./div/div[2]/div[1]/a/span[1]/text()'))  # 电影标题
                src = self.get_first_text(li.xpath('./div/div[2]/div[1]/a/@href'))  # 电影链接

                # 获取并解析导演和主演信息
                info_text = self.get_first_text(li.xpath('./div/div[2]/div[2]/p[1]/text()'))
                if '主演:' in info_text:
                    director_part, actor_part = info_text.split('主演:', 1)
                    director = director_part.replace('导演:', '').strip()
                    actors = actor_part.strip()
                else:
                    director = info_text.replace('导演:', '').strip()
                    actors = ''

                year_country_kond = self.get_first_text(li.xpath('./div/div[2]/div[2]/p[1]/text()[2]'))  # 年份、国家、类型
                year, country, type = self.spiltCountry(year_country_kond)

                score = self.get_first_text(li.xpath('./div/div[2]/div[2]/div/span[2]/text()'))  # 评分
                comment = self.get_first_text(li.xpath('./div/div[2]/div[2]/div/span[4]/text()'))  # 评价人数
                summary = self.get_first_text(li.xpath('./div/div[2]/div[2]/p[2]/span/text()'))  # 电影简介

                #print(count, title, src, director, actors, year_country_kond, score, comment, summary)  # 输出

                # 构建数据字典
                data = {
                    "序号": count,
                    "标题": title,
                    "链接": src,
                    "导演": director,
                    "主演": actors,
                    "年份": year,
                    "国家": country,
                    "类型": type,
                    "评分": score,
                    "评价人数": comment,
                    "简介": summary
                }

                data_list.append(data)
                count += 1

        return data_list  # 返回所有解析后的数据

    # 数据可视化（保持原有功能不变）
    def visualization(self):
        csv_file_path = self.file_name + ".csv"
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"文件 {csv_file_path} 不存在，请先运行 run() 方法生成数据文件。")

        df = pd.read_csv(csv_file_path)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))  # 1行2列，画布尺寸15x6

        ## 图1. 绘制电影 年份 饼形图 ，只获取前 20 个份额最大的年份，剩下的用 其他 表示
        df_copy = df.copy()  # 拷贝一份数据
        front = 20
        year_counts = df['年份'].value_counts()
        top_years = year_counts.head(front)
        other_years = year_counts[front:]
        top_years['其他'] = other_years.sum()
        ax1.pie(top_years, labels=top_years.index, autopct='%1.1f%%')
        ax1.legend(bbox_to_anchor=(1, 0.5))  # 图例放在右侧
        ax1.set_title('电影年份分布（Top 20）')

        ## 图2. 绘制电影 国家 饼形图 ，只获取前 15 个份额最大的国家，剩下的用 其他 表示，一个电影可能包含多个国家，国家列中使用空格分开的
        df_copy = df.copy()  # 拷贝一份数据
        front = 15
        country_counts = df['国家'].str.split(' ').explode().value_counts()
        top_countries = country_counts.head(front)
        other_countries = country_counts[front:]
        top_countries['其他'] = other_countries.sum()
        ax2.pie(top_countries, labels=top_countries.index, autopct='%1.1f%%')
        ax2.legend(bbox_to_anchor=(1, 0.5))  # 图例放在右侧
        ax2.set_title('电影国家分布（Top 15）')

        # 调整布局并保存
        plt.tight_layout()  # 防止标题重叠
        plt.savefig('电影年份与国家分布.png')
        plt.show()

        ## 图3. 绘制电影评分和评价人数之间的散点图，比较他们之间的关系。
        df_copy = df.copy()  # 拷贝一份数据
        df_copy['评分'] = df_copy['评分'].astype(float)  # 转换为浮点数
        df_copy['评价人数'] = df_copy['评价人数'].str.replace('人评价', '').astype(int)  # 去掉后面的“人评价”，转换为整数

        plt.scatter(df_copy['评分'], df_copy['评价人数'])  # 绘制散点图
        plt.xlabel('评分')  # X轴标签
        plt.ylabel('评价人数')  # Y轴标签
        plt.title('电影评分和评价人数之间的散点图')  # 标题
        plt.savefig('电影评分和评价人数之间的散点图.png')  # 保存图片
        plt.show()  # 显示图片

        # 绘制电影 类型 和 国家 这两列数据公共的词云图，使用空格拆分
        # 指定中文字体路径（替换为你的实际路径）
        font_path = 'msyh.ttc'
        all_types = ' '.join(df['类型'].str.split(' ').explode())

        # 生成词云（关键：添加 font_path 参数）
        wordcloud = WordCloud(
            width=800, height=400, background_color='white',
            font_path=font_path  # 指定中文字体
        ).generate(all_types)

        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('电影类型词云图', fontproperties=font_path)  # 标题也使用中文字体
        plt.savefig('电影类型词云图.png')
        plt.show()


# 保留原有入口（可选，用于单独测试爬虫功能）
# if __name__ == '__main__':
#     doubanSpider = DoubanSpider()
#     data = doubanSpider.run()
#     print(f"成功爬取 {len(data)} 条电影数据")
    # 注意：这里不会自动保存文件，如需保存可调用原有的 to_excel/to_csv 方法
    # df = pd.DataFrame(data)
    # df.to_excel("豆瓣电影top250数据.xlsx", index=False)