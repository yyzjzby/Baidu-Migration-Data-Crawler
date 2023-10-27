import pandas as pd
import numpy as np
from tqdm import tqdm
import os

def main(path,reader,d):
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

if __name__ == '__main__':
    path = r".\市到市转换后"
    d="2023-01-01"#qishi
    reader = pd.read_csv(r".\标准OD顺序.csv",encoding="gbk")["O"].to_list()
    kk = main(path,reader,d)
    kk.to_csv(r"汇总OD.csv",encoding="gbk")