from tqdm import tqdm
import spider as msd
import datetime  # 导入datetime模块
import pandas as pd
import numpy as np
import csv
import os
from codedict import CitiesCode,ProvinceCode  #调用城市编码字典


def xin_jisuan(start,end,p,path1,c):
    if not os.path.exists(r'./temp1'):
        os.makedirs(r'./temp1')
    else:
        pass
    city = pd.read_json(p + '.json')
    NameList = list(city['地级市名称'].unique())
    NameList1 = list(city['省名称'].unique())
    global dfs
    dfs = []
    panduan = []
    for nnnnn in tqdm(NameList):
        try:
            df = pd.read_csv(path1+"\\"+nnnnn+".csv")
        except:
            print(nnnnn)
            continue
        dfs.append(df)
        panduan.append(nnnnn)
    global mm
    if c == 'out':
        mm = '出'
    elif c == 'in':
        mm = '入'
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_list.append(datestart.strftime('%Y-%m-%d'))
    # title = date_list
    # data1:迁出 data:迁入
    for kk in NameList1:
        for nn in tqdm(NameList):
            # reader = pd.ExcelFile("test-市到市.xlsx")
            if os.path.exists(r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)):
                if not os.path.exists(r'./temp1/{}to{}_{}_{}_{}有效计算.csv'.format(start, end, p, nn, mm)):
                    try:
                        path = r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)
                        data = pd.read_csv(path)
                    except:
                        pass
                    # index = data[data.date == bb].index.tolist()[0]
                    data1 = data
                    data = np.array(data)
                    # oo = -1
                    title = []
                    ll = []
                    for b in date_list:
                        try:
                            # oo += 1
                            ii = dfs[panduan.index(nn)][b] * 0.01 * data[data1[data1.date == int(b.replace("-",""))].index.tolist()[0]][1]
                            ll.append(ii)
                            with open(r'./temp1/{}to{}_{}_{}_{}有效计算.csv'.format(start, end, p, nn, mm), 'w') as csvFile:
                                writer = csv.writer(csvFile, lineterminator='\n')
                                writer.writerows(ll)
                            title.append(b)
                        except:
                            pass
                else:
                    pass
            else:
                pass
    for nnn in tqdm(NameList):
        if not os.path.exists(r'./temp1/{}to{}_{}_{}_{}有效计算_转置.csv'.format(start, end, p, nnn, mm)):
            try:
                df = pd.read_csv(r'./temp1/{}to{}_{}_{}_{}有效计算.csv'.format(start, end, p, nnn, mm))
                data = df.values
                index1 = list(df.keys())
                data = list(map(list, zip(*data)))
                data = pd.DataFrame(data, index=index1)
                data.to_csv(r'./temp1/{}to{}_{}_{}_{}有效计算_转置.csv'.format(start, end, p, nnn, mm), header=0)
            except:
                print(nnn)
                pass
        else:
            pass
    pd.DataFrame(title).to_csv("表头.csv", encoding="gbk")



def zhuan(start,end,p,c):
    if not os.path.exists(r'./市到市转换后'):
        os.makedirs(r'./市到市转换后')
    else:
        pass
    if c=='out':
        mm = '出'
    elif c == 'in':
        mm = '入'
    city = pd.read_json(p + '.json')
    NameList = list(city['地级市名称'].unique())
    for nnnn in tqdm(NameList):
        try:
            df = pd.read_csv(r'./temp1/{}to{}_{}_{}_{}有效计算_转置.csv'.format(start, end, p, nnnn, mm), header=None)
            df = df.rename_axis('index').reset_index()
        except:
            print(nnnn)
            continue
        # print(df)
        try:
            df1 = pd.read_csv(r'./ceshi1/{}.csv'.format(nnnn)).iloc[:,:5]
            df1 =  df1.rename_axis('index').reset_index()
            # print(df1)
            df2 = pd.DataFrame()
            df2 = pd.merge(df1, df)
            df2.to_csv(r"./市到市转换后/"+"-"+nnnn+".csv",encoding="utf-8_sig")
        except:
            print(nnnn)
            pass

# 用来计算比率
if __name__ == '__main__':
    start = '2023-01-01'
    end = '2023-05-31'
    p = 'CityCode'
    path1= r"./ceshi1"
    xin_jisuan(start,end,p,path1,"in")
    zhuan(start,end,p,"in")


