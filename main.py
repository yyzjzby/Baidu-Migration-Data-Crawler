#coding:utf-8
import requests  # 导入请求模块
import json   # 导入json模块
import csv    # 导csv入模块
import os     # 导入os模块
import shutil  # 导入shutil模块
import time    # 导入时间模块
import datetime  # 导入datetime模块
from codedict import CitiesCode,ProvinceCode  #调用城市编码字典

headers = {
    'Host': 'huiyan.baidu.com',
    'Referer': 'https://qianxi.baidu.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3870.400 QQBrowser/10.8.4405.400'
} # 定义请求头信息

def get_time():
    # begin_date = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y%m%d")  #获取31天前的日期
    # date_list = []   #定义一个存放日期的空列表
    # begin_date = datetime.datetime.strptime(begin_date, "%Y%m%d")  # 将字符串开始日期转成日期格式
    # end_date = datetime.datetime.strptime(time.strftime('%Y%m%d',time.localtime(time.time())), "%Y%m%d")  # 将字符串开始日期转成日期格式，time.localtime作用是格式化时间戳为本地的时间
    # while begin_date <= end_date:   #建立while循环获取近30天的日期
    #     date_str = begin_date.strftime("%Y%m%d")  #从获取第一天的日期起，依次获取下一天的日期
    #     date_list.append(date_str)   #将获取的日期存放到date_list列表中
    #     begin_date += datetime.timedelta(days=1)   #从上一个日期进入下一个日期
    start = '20220101'
    end = '20220425'
    datestart = datetime.datetime.strptime(start, '%Y%m%d')
    dateend = datetime.datetime.strptime(end, '%Y%m%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_list.append(datestart.strftime('%Y%m%d'))
    url = 'https://huiyan.baidu.com/migration/lastdate.jsonp?'   # 通过此url获取获取百度地图慧眼最新数据的日期
    response = requests.get(url, headers=headers, timeout=30)  # 发出请求并json化处理
    lastdate = response.text[-12:-4]   # 从字符串中提取出日期
    datetime_list = []  # 定义一个存放有用日期的空列表
    for i in date_list:    #通过for循环筛选出有用的日期
        if i == lastdate:
            datetime_list.append(lastdate)  # 将最新日期存放到datetime_list列表中
            break
        else:
            datetime_list.append(i)   # 将最新日期之前的日期存放到datetime_list列表中
    return datetime_list  # 返回datetime_list

def city_move(): #全国地级市迁徙数据
    date_list = get_time()  # 调用datetime_list列表
    print(date_list)
    directions = ['in', 'out']  #迁徙方向
    level_dict = {'市级':'city', '省级':'province'} #数据级别
    for name, city_id in CitiesCode.items():  #遍历城市名称和代号
        for Dir in directions:  #遍历迁徙方向
            for Le,le in level_dict.items(): #遍历数据级别
                for date in date_list:  #遍历日期
                   #通过if-else语句定义dir
                    if Dir == 'in':
                        dir = '迁入'
                    else:
                        dir = '迁出'

                    if city_id % 10000 == 0:  # 除了地级行政区，其余的编码%10000都为0
                        province = name
                        continue
                    # 判断是否存在csv文件，如存在，结束当前循环
                    if os.path.exists('全国迁徙趋势\{}\{}\{}\{}{}\{}{}{}{}.csv'.format(province, name, dir, Le, dir,name, date, Le, dir)):
                        continue
                    # 获取不同的url
                    url = 'https://huiyan.baidu.com/migration/{}rank.jsonp?dt=city&id={}&type=move_{}&date={}'.format(le, city_id, Dir,date)
                    print(f'{name}:{url}')
                    response = requests.get(url, headers=headers,timeout=30)  # 发出请求并json化处理
                    # 判断多级目录是否存在，不存在则创建
                    if not os.path.exists("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码".format(province, name, dir, Le, dir)):
                        os.makedirs("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码".format(province, name, dir, Le, dir))
                    with open("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码\{}{}{}{}.txt".format(province, name, dir, Le, dir,name, date, Le, dir), 'w',encoding='utf-8') as fp:
                        fp.write(response.text)  # 保存网页源代码
                    fp.close()
                    time.sleep(1)  # 挂起一秒
                    r = response.text[4:-1]  # 去头去尾
                    data_dict = json.loads(r)  # json化
                    if data_dict['errno'] == 0:
                        data_list = data_dict['data']['list']
                        if Le == '市级':
                            # 打开并建立如‘广州市20210612市级迁入.csv’的文件
                            with open("{}{}{}{}.csv".format(name, date, Le, dir), "w+", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                header = ["city_name", "province_name", "value"]  # 定义表头
                                writer.writerow(header)  # 把表头写入
                                for i in range(len(data_list)):
                                    city_name = data_list[i]['city_name']  # 城市名
                                    province_name = data_list[i]['province_name']  # 省份名
                                    value = data_list[i]['value'] # 迁徙数据
                                    writer.writerow([city_name, province_name, value])  # 写入信息
                            csv_file.close()   # 关闭，很重要,确保不要过多的链接
                            # 定义目录
                            dst = r"全国迁徙趋势\{}\{}\{}\{}{}".format(province, name, dir, Le, dir)
                            # 移动csv文件到指定目录，实现自动分类
                            shutil.move("{}{}{}{}.csv".format(name, date, Le, dir), dst)
                        else:  # 此次注释同上
                            with open("{}{}{}{}.csv".format(name, date, Le, dir), "w+", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                header = ["province_name", "value"]
                                writer.writerow(header)
                                for i in range(len(data_list)):
                                    province_name = data_list[i]['province_name']
                                    value = data_list[i]['value']
                                    writer.writerow([province_name, value])
                            csv_file.close()
                            dst = r"全国迁徙趋势\{}\{}\{}\{}{}".format(province, name, dir, Le, dir)
                            shutil.move("{}{}{}{}.csv".format(name, date, Le, dir), dst)

def province_move(): #全国省份迁徙数据
    date_list = get_time()
    print(date_list)
    directions = ['in', 'out']
    level_dict = {'市级':'city', '省级':'province'}
    for name, city_id in ProvinceCode.items():
        for Dir in directions:  #遍历迁徙方向
            for Le,le in level_dict.items():
                for date in date_list:
                   #通过if-else语句定义dir
                    if Dir == 'in':
                        dir = '迁入'
                    else:
                        dir = '迁出'

                    if city_id % 10000 == 0:  # 除了地级行政区，其余的编码%10000都为0
                        province = name
                    # 判断是否存在csv文件，如存在，结束当前循环
                    if os.path.exists('全国迁徙趋势\{}\{}\{}\{}{}\{}{}{}{}.csv'.format(province, name, dir, Le, dir,name, date, Le, dir)):
                        continue
                    # 获取不同的url
                    url = 'https://huiyan.baidu.com/migration/{}rank.jsonp?dt=province&id={}&type=move_{}&date={}'.format(le, city_id, Dir,date)
                    print(f'{name}:{url}')
                    response = requests.get(url, headers=headers,timeout=30)  # 发出请求并json化处理
                    # 判断多级目录是否存在，不存在则创建
                    if not os.path.exists("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码".format(province, name, dir, Le, dir)):
                        os.makedirs("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码".format(province, name, dir, Le, dir))
                    with open("全国迁徙趋势\{}\{}\{}\{}{}\网页源代码\{}{}{}{}.txt".format(province, name, dir, Le, dir,name, date, Le, dir), 'w',encoding='utf-8') as fp:
                        fp.write(response.text)  # 保存网页源代码
                    fp.close()
                    time.sleep(1)  # 挂起一秒
                    r = response.text[4:-1]  # 去头去尾
                    data_dict = json.loads(r)  # 字典化
                    if data_dict['errno'] == 0:
                        data_list = data_dict['data']['list']
                        if Le == '市级':
                            # 打开并建立如‘广州市20210612市级迁入.csv’的文件
                            with open("{}{}{}{}.csv".format(name, date, Le, dir), "w+", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                header = ["city_name", "province_name", "value"]  # 定义表头
                                writer.writerow(header)  # 把表头写入
                                for i in range(len(data_list)):
                                    city_name = data_list[i]['city_name']  # 城市名
                                    province_name = data_list[i]['province_name']  # 省份名
                                    value = data_list[i]['value'] # 迁徙数据
                                    writer.writerow([city_name, province_name, value])  # 写入信息
                            csv_file.close()   # 关闭，很重要,确保不要过多的链接
                            # 定义目录
                            dst = r"全国迁徙趋势\{}\{}\{}\{}{}".format(province, name, dir, Le, dir)
                            # 移动csv文件到指定目录，实现自动分类
                            shutil.move("{}{}{}{}.csv".format(name, date, Le, dir), dst)
                        else:  # 此次注释同上
                            with open("{}{}{}{}.csv".format(name, date, Le, dir), "w+", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                header = ["province_name", "value"]
                                writer.writerow(header)
                                for i in range(len(data_list)):
                                    province_name = data_list[i]['province_name']
                                    value = data_list[i]['value']
                                    writer.writerow([province_name, value])
                            csv_file.close()
                            dst = r"全国迁徙趋势\{}\{}\{}\{}{}".format(province, name, dir, Le, dir)
                            shutil.move("{}{}{}{}.csv".format(name, date, Le, dir), dst)

def city_trend():  # 全国迁徙趋势
    directions = ['in', 'out']
    for name, city_id in CitiesCode.items():
        for direction in directions:
            if direction == 'in':
                dir = '迁入'
            else:
                dir = '迁出'
            if city_id % 10000 == 0:
                province = name
                continue
            if os.path.exists('全国迁徙趋势\{}\{}\{}\{}{}趋势.csv'.format(province, name, dir, name, dir)):
                continue
            url='https://huiyan.baidu.com/migration/historycurve.jsonp?dt=city&id={}&type=move_{}'.format(city_id, direction)
            print(f'{name}:{url}')
            response = requests.get(url, headers=headers,timeout=30)  # 发出请求并json化处理
            # 判断多级目录是否存在，不存在则创建
            if not os.path.exists("全国迁徙趋势\{}\{}\{}\网页源代码".format(province, name, dir)):
                os.makedirs("全国迁徙趋势\{}\{}\{}\网页源代码".format(province, name, dir))
            with open("全国迁徙趋势\{}\{}\{}\网页源代码\{}{}趋势.txt".format(province, name, dir, name, dir), 'w', encoding='utf-8') as fp:
                fp.write(response.text)  # 保存网页源代码
            fp.close()
            time.sleep(1)
            r = response.text[4:-1]
            data_dict = json.loads(r)
            if data_dict['errno'] == 0:
                data_list = data_dict['data']['list']
                keys = list(data_list.keys())[-1000:]
                values = list(data_list.values())[-1000:]
                with open("{}{}趋势.csv".format(name, dir), "w+", newline="") as csv_file:
                    writer = csv.writer(csv_file)
                    header =["date", "value"]
                    writer.writerow(header)
                    for i in range(len(keys)):
                        date = keys[i]
                        value = values[i]
                        writer.writerow([date, value])
                csv_file.close()
                if not os.path.exists("全国迁徙趋势\{}\{}\{}".format(province, name, dir)):
                    os.makedirs("全国迁徙趋势\{}\{}\{}".format(province, name, dir))
                dst = r"全国迁徙趋势\{}\{}\{}".format(province, name, dir)
                shutil.move("{}{}趋势.csv".format(name, dir), dst)

def province_trend():  # 迁徙趋势
    directions = ['in', 'out']
    for name, city_id in ProvinceCode.items():
        for direction in directions:
            if direction == 'in':
                dir = '迁入'
            else:
                dir = '迁出'
            if city_id % 10000 == 0:
                province = name
            if os.path.exists('全国迁徙趋势\{}\{}\{}\{}{}趋势.csv'.format(province, name, dir, name, dir)):
                continue
            url='https://huiyan.baidu.com/migration/historycurve.jsonp?dt=province&id={}&type=move_{}'.format(city_id, direction)
            print(f'{name}:{url}')
            response = requests.get(url, headers=headers,timeout=30)  # 发出请求并json化处理
            # 判断多级目录是否存在，不存在则创建
            if not os.path.exists("全国迁徙趋势\{}\{}\{}\网页源代码".format(province, name, dir)):
                os.makedirs("全国迁徙趋势\{}\{}\{}\网页源代码".format(province, name, dir))
            with open("全国迁徙趋势\{}\{}\{}\网页源代码\{}{}趋势.txt".format(province, name, dir, name, dir), 'w', encoding='utf-8') as fp:
                fp.write(response.text)  # 保存网页源代码
            fp.close()
            time.sleep(1)
            r = response.text[4:-1]
            data_dict = json.loads(r)
            if data_dict['errno'] == 0:
                data_list = data_dict['data']['list']
                keys = list(data_list.keys())[-1000:]
                values = list(data_list.values())[-1000:]
                with open("{}{}趋势.csv".format(name, dir), "w+", newline="") as csv_file:
                    writer = csv.writer(csv_file)
                    header =["date", "value"]
                    writer.writerow(header)
                    for i in range(len(keys)):
                        date = keys[i]
                        value = values[i]
                        writer.writerow([date, value])
                csv_file.close()
                if not os.path.exists("全国迁徙趋势\{}\{}\{}".format(province, name, dir)):
                    os.makedirs("全国迁徙趋势\{}\{}\{}".format(province, name, dir))
                dst = r"全国迁徙趋势\{}\{}\{}".format(province, name, dir)
                shutil.move("{}{}趋势.csv".format(name, dir), dst)

# 用于爬取趋势
if __name__ == '__main__':
    print('开始爬取...')
    # city_move()
    # province_move()
    #通过改codedict.py进行选择相应的地级市，获取其人口迁徙的规模，然后去净增excel.py
    city_trend()
    province_trend()
    print('全部完成...')





