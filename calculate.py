from tqdm import tqdm
import spider as msd
import datetime  # 导入datetime模块
import pandas as pd
import numpy as np
import csv
import os
from codedict import CitiesCode,ProvinceCode  #调用城市编码字典

def he(path):
    reader = os.listdir(path)
    writer = pd.ExcelWriter('2022市到市.xlsx')
    for sheet in tqdm(reader):
        df = pd.read_csv(path + "/" +sheet)
        df.to_excel(writer,sheet_name=str(sheet)[:-4])
    writer.save()


def jisuan(start,end,p,path1,bb,c):
    i = 0
    city = pd.read_json(p+'.json')
    NameList = list(city['地级市名称'].unique())
    global reader
    reader = pd.ExcelFile("test-市到市.xlsx")
    global dfs
    dfs = []
    for nnnnn in tqdm(NameList):
        df = reader.parse(sheet_name=nnnnn)
        dfs.append([df.nnnnn])
    global mm
    if c=='out':
        mm = '出'
    elif c == 'in':
        mm = '入'
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    date_list = []
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        date_list.append(datestart.strftime('%Y-%m-%d'))
    # data1:迁出 data:迁入
    province1 = []
    for name, city_id in CitiesCode.items():
            if city_id % 10000 == 0:
                province = name
                province1.append(province)
                continue
    for kk in province1:
        for nn in tqdm(NameList):
            # reader = pd.ExcelFile("test-市到市.xlsx")
            if os.path.exists(r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)):
                try:
                    print(kk+"-"+nn)
                    path = r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)
                    print(path)
                    data = pd.read_csv(path)
                except:
                    pass
                index = data[data.date == bb].index.tolist()[0]
                data = np.array(data)
                oo = -1
                ll = []
                for b in tqdm(date_list):
                    try:
                        oo += 1
                        ii = dfs[i][b] * 0.01 * data[index + oo][1]
                        ll.append(ii)
                        with open(r'./temp/{}to{}_{}_{}_{}有效计算.csv'.format(start,end,p,nn,mm), 'w') as csvFile:
                            writer = csv.writer(csvFile, lineterminator='\n')
                            writer.writerows(ll)
                    except:
                        print(b)
                i += 1
            else:
                pass
    for nnn in tqdm(NameList):
        try:
            df = pd.read_csv(r'./temp/{}to{}_{}_{}_{}有效计算.csv'.format(start,end,p,nnn,mm))
            data = df.values
            index1 = list(df.keys())
            data = list(map(list, zip(*data)))
            data = pd.DataFrame(data, index=index1)
            data.to_csv(r'./temp/{}to{}_{}_{}_{}有效计算_转置.csv'.format(start, end, p, nnn, mm), header=0)
        except:
            print(nnn)
            pass



def xin_jisuan(start,end,p,path1,bb,c):
    city = pd.read_json(p + '.json')
    NameList = list(city['地级市名称'].unique())
    NameList1 = list(city['省名称'].unique())
    global reader
    reader = pd.ExcelFile("test-市到市.xlsx")
    global dfs
    dfs = []
    panduan = []
    for nnnnn in tqdm(NameList):
        try:
            df = reader.parse(sheet_name=nnnnn)
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
    # data1:迁出 data:迁入
    for kk in NameList1:
        for nn in tqdm(NameList):
            # reader = pd.ExcelFile("test-市到市.xlsx")
            if os.path.exists(r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)):
                try:
                    path = r'全国迁徙趋势\{}\{}\迁{}\{}迁{}趋势.csv'.format(kk, nn, mm, nn, mm)
                    data = pd.read_csv(path)
                except:
                    pass
                index = data[data.date == bb].index.tolist()[0]
                data = np.array(data)
                oo = -1
                ll = []
                for b in tqdm(date_list):
                    try:
                        oo += 1
                        ii = dfs[panduan.index(nn)][b] * 0.01 * data[index + oo][1]
                        ll.append(ii)
                        with open(r'./temp1/{}to{}_{}_{}_{}有效计算.csv'.format(start, end, p, nn, mm), 'w') as csvFile:
                            writer = csv.writer(csvFile, lineterminator='\n')
                            writer.writerows(ll)
                    except:
                        print(b)
            else:
                pass
    for nnn in tqdm(NameList):
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


def zhuanhuan(start,end,p,searchKey,c):
    global bg
    # bg = op.load_workbook("test-市到市.xlsx")
    reader = pd.ExcelFile("test-市到市.xlsx")
    if c=='out':
        mm = '出'
    elif c == 'in':
        mm = '入'
    city = pd.read_json(p + '.json')
    NameList = list(city[searchKey].unique())
    print("正在修改原Ecxel数据........")
    for nnnn in tqdm(NameList):
        df = pd.read_csv(r'./temp1/{}to{}_{}_{}_{}有效计算_转置.csv'.format(start, end, p, nnnn, mm), header=None)
        data = df.values
        # bg = op.load_workbook("test-市到市.xlsx")
        try:
            df = reader.parse(sheet_name=nnnn)
            print(df)
            # sheet = bg.sheet_by_name(nnnn)
            # sheet = bg[nnnn]
            for i in range(1, data.shape[0] + 1):
                for j in range(1, data.shape[1] + 1):
                    pass
                    # df.cell(1 + i, 6 + j, data[-1 + i][j - 1])
                    # sheet.cell(1 + i, 6 + j, data[-1 + i][j - 1])
                    # bg.save("test1.xlsx")
        except:
            print(nnnn)
            pass


def zhuan(start,end,p,c):
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
            df1 = pd.read_csv(r'./市到市空值变0/{}.csv'.format(nnnn)).iloc[:,:5]
            df1 =  df1.rename_axis('index').reset_index()
            # print(df1)
            df2 = pd.DataFrame()
            df2 = pd.merge(df1, df)
            df2.to_csv(r"./市到市转换后/"+"-"+nnnn+".csv",encoding="utf-8_sig")
        except:
            print(nnnn)
            pass

if __name__ == '__main__':
    start = '2021-09-13'
    end = '2022-11-20'
    p = 'CityCode'
    path1= r"./ceshi2"
    he(path1)
    xin_jisuan(start,end,p,path1,20210914,"in")
    # zhuan(start,end,p,"in")
    # he(r"./市到市转换后")

