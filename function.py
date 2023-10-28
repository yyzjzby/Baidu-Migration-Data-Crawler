import datetime  # 导入datetime模块
import pandas as pd
import numpy as np
import csv
from tqdm import tqdm
import os
import spider as msd

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
                    title= []
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
    # print(title)
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

def make_od(path,reader,d):
    uu=[]
    for sheet in tqdm(reader):
        df = pd.read_csv(path + "\\-" + sheet+".csv")
        # print(sheet)
        ll = df.drop(["Unnamed: 0", "index", "Unnamed: 0.1", "Unnamed: 0.1.1", "o_city", d,"0"], axis=1)
        ll["city_name"] = df["city_name"]+"-"+sheet
        uu.append(ll)
    hh = pd.read_csv(path+"\\-七台河市.csv")
    hh = hh.drop(["Unnamed: 0", "index", "Unnamed: 0.1", "Unnamed: 0.1.1", "o_city", d,"0"], axis=1)
    kk = hh.columns.to_list()
    gg = pd.DataFrame(columns=kk)
    for i in tqdm(uu):
        gg = gg.append(i)
    return gg

def paqu(start_date, end_date, searchKey, migrationType, o_level, d_level,p,path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass
    city = pd.read_json(p+'.json')
    NameList = list(city[searchKey].unique())
    start_date = start_date
    end_date = end_date
    global dateRange
    dateRange = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
    for i in tqdm(NameList):
        a = msd.GetBatchDateData(o_level, d_level, i, migrationType=migrationType,date=dateRange,cityCodePath=p+".json")
        a.to_csv(path+"\\"+i+".csv",encoding="utf-8_sig")

def zhuanOD(mm,df):
    if not os.path.exists(r'.\各日OD'):
        os.makedirs(r'.\各日OD')
    else:
        pass
    jj = pd.DataFrame()
    jj["O"]=df["O"]
    jj["D"]=df["D"]
    jj[mm]=df[mm].astype("float")
    oo=jj.pivot_table(index="O",columns="D",values=mm)
    oo=oo.fillna(0)
    # mm=mm.replace("/","-")
    oo.to_csv(".\各日OD\\"+mm+".csv",encoding="gbk")