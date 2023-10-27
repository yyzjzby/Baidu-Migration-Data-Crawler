import json

def CityName2Code(dt, cityName,cityCodePath):
    """城市名/省名转换为编码
    Arguments:
        dt {str} -- [description]
        cityName {str} -- [description]
    """
    # 城市编码的相对路径
    # cityCodePath = '.json'
    # 打开文件，文件编码格式为UTF-8
    data = open((cityCodePath), encoding='utf-8')
    result = json.load(data)
    if dt == 'province':
        searchKey = '省名称'
        codeKey = '省代码'
    elif dt == 'city':
        searchKey = '地级市名称'
        codeKey = '地级市代码'
    for rowNum in range(len(result)):
        if result[rowNum][searchKey] == cityName:
            cityCode = result[rowNum][codeKey]
    return cityCode

