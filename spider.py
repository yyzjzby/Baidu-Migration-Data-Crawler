import requests
import json
import time
import pandas as pd
import CityTransform
from tqdm import tqdm
from retry import retry


def JsonTextConvert(text):
    """Text2Json

    Arguments:
        text {str} -- webContent

    Returns:
        str -- jsonText
    """
    text = text.encode('utf-8').decode('unicode_escape')
    head, sep, tail = text.partition('(')
    tail = tail.replace(")", "")
    return tail

@retry()
def GetBatchDateData(rankMethod, dt, name, migrationType, date,cityCodePath):
    cityID = CityTransform.CityName2Code(dt, name,cityCodePath)
    _df = None
    for day in tqdm(date):
        try:
            timeArray = time.strptime(day + " 00:00:00", "%Y-%m-%d %H:%M:%S")
                # 转换成时间戳
            timeUnix = time.mktime(timeArray)
            kw = {'dt': dt, 'id': cityID, 'type': "move_" +migrationType, 'date': day.replace('-', ''), "callback": 'jsonp_'+str(int(timeUnix))+'000_0000000'}
            url = 'https://huiyan.baidu.com/migration/{0}rank.jsonp'.format(rankMethod)
            r = requests.request('GET', url, params=kw)  # 添加到URL中
            text = r.text
            rawData = json.loads(JsonTextConvert(text))
            data = rawData['data']
            l = data['list']
            if l == []:
                print(day)
                continue
            else:
                df = pd.DataFrame(l)
                df.rename(columns={'value': day, 'province_name': 'o_city'}, inplace=True)
                df['d_city'] = name
                if _df is None:
                    _df = df
                else:
                    _df = pd.merge(_df, df,how="outer")
        except:
            print(day)
            pass
    return _df


def GetData(rankMethod, dt, name, migrationType, date, isExcel):
    """
    Arguments:
        rankMethod {str} -- city||province 获得数据的行政级别
        dt {str} -- city||province 中心地行政级别
        name {str} -- example:'温州市||浙江省' 作为中心地的地名
        migrationType {str} -- in||out
        date {} -- example:2020-02-02
        isExcel {bool} -- true转出为excel格式
    """
    cityID = CityTransform.CityName2Code(dt, name)
    # 转换成时间数组
    timeArray = time.strptime(date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    # 转换成时间戳
    timeUnix = time.mktime(timeArray)
    kw = {'dt': dt, 'id': cityID, 'type': "move_" + migrationType, 'date': date.replace('-', ''), "callback": 'jsonp_'+str(int(timeUnix))+'000_0000000'}
    url = 'https://huiyan.baidu.com/migration/{0}rank.jsonp'.format(rankMethod)
    r = requests.request('GET', url, params=kw)  # 添加到URL中
    text = r.text
    rawData = json.loads(JsonTextConvert(text))
    data = rawData['data']
    l = data['list']
    df = pd.DataFrame(l)
    if isExcel == True:
        df.to_excel(name + date+'.xlsx')
    else:
        return df


def main():
    cc= []
    dd = GetBatchDateData('city', 'city', '南京市', migrationType='in',date=['2022-06-01','2022-06-02'],cityCodePath="CityCode.json")
    ddd = GetBatchDateData('city', 'city', '苏州市', migrationType='in', date=['2022-06-01', '2022-06-02'],cityCodePath="CityCode.json")
    cc = cc.append(dd,ddd)
    print(cc)
    pass

#
if __name__ == '__main__':
    main()
