from tqdm import tqdm
import pandas as pd
import os
from function import xin_jisuan,zhuan,make_od,paqu,zhuanOD

def tihuanshoudong(DataFrame):
    df = DataFrame
    df2 = pd.read_csv("表头.csv")
    biaotou = df2["0"].to_list()[1:]
    new_columns = df['city_name'].str.split('-', expand=True)
    df = pd.concat([new_columns, df], axis=1)
    df = df.drop(columns=["city_name"])
    ll = biaotou
    kk = ["O", "D"]
    kk = kk + ll
    kk = [s.replace("/", "-") for s in kk]
    df.columns = kk
    return df


def run():
    p = 'CityCode'
    path = "./ceshi1"
    migrationType = 'in'
    print("----------------开始爬取-------------------------")
    paqu(start, end, '地级市名称', migrationType, 'city', 'city',p,path)
    print("----------------空值填0--------------------------")
    reader = os.listdir(path)
    for sheet in tqdm(reader):
        df = pd.read_csv(path + "/" + sheet)
        df = df.fillna(0)
        df.to_csv(path+"\\" + sheet, encoding="utf-8_sig")
    path1= r"./ceshi1"
    xin_jisuan(start,end,p,path1,"in")
    zhuan(start,end,p,"in")
    print("--------------预处理-------------------")
    path2 = r".\市到市转换后"
    reader = pd.read_csv(r".\标准OD顺序.csv",encoding="gbk")["O"].to_list()
    kk = make_od(path2,reader,start)
    kkk = tihuanshoudong(kk)
    kk.to_csv(r"汇总OD.csv",encoding="gbk",index=False)
    kkk.to_csv(r"汇总OD_转.csv",encoding="gbk",index=False)
    df = pd.read_csv(r"汇总OD_转.csv",encoding = "gbk")
    print("--------------转OD---------------------")
    df1 = df.columns.to_list()
    vv = df1[2:]
    for i in tqdm(vv):
        zhuanOD(i,df)
    print("爬取完成")

if __name__ == '__main__':
    start = '2023-08-11'
    end = '2023-10-25'
    run()


