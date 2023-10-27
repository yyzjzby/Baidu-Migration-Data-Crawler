import pandas as pd
import os
from tqdm import tqdm

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

if __name__ == '__main__':
    df = pd.read_csv(r"汇总OD.csv", encoding="gbk")
    df1 = df.columns.to_list()
    vv = df1[2:]
    for i in tqdm(vv):
        zhuanOD(i,df)