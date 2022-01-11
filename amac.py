import requests
import random
import json
import time
from pymongo import MongoClient
from bs4 import BeautifulSoup
from rdflib import Graph, Literal
from pyfuseki import FusekiUpdate
import pyfuseki.register
from pyfuseki.utils import RdfUtils
from rdflib import URIRef as uri, RDF, XSD
from pyfuseki.rdf import rdf_prefix, rdf_property, NameSpace as ns

COMMON_PREFIX = 'http://www.inet.com/kg/ontoligies/ifa#'
pyfuseki.register.register_common_prefix(COMMON_PREFIX)

@rdf_prefix
class FirmRdfPrefix:
    """
    公司信息三元组前缀的枚举
    用于向graph中bind前缀，该枚举的name为前缀，value为其对应的包装了IRI的URIRef或Namespace对象
    """
    PrivateFundManager: ns
    FundProduct: ns


rp = FirmRdfPrefix()


@rdf_property
class DataProperty:
    """
    本体中所有Data properties的枚举
    name 为该 property 的 display name， value 为包装了该 property IRI 的 URIRef 对象
    """
    hasName: uri
    brandAgencyDataProperty: uri
    createTime: uri
    enName: uri


dp = DataProperty()


@rdf_property
class ObjectProperty:
    """
    本体中所有Object properties的枚举
    name 为该 property 的 display name， value 为包装了该 property IRI 的 URIRef 对象
    """
    brandAgencyObjectProperty: uri
    subordinateTo: uri   # 从属于
    hasFounder: uri  # 有创始人


op = ObjectProperty()

headers = {'Host': 'gs.amac.org.cn',
           'Accept': 'application/json, text/javascript, */*; q=0.01',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6',
           'Connection': 'close',
           'X-Requested-With': 'XMLHttpRequest',
           'Content-Type': 'application/json',
           'Origin': 'https://gs.amac.org.cn',
           'Referer': 'https://gs.amac.org.cn/amac-infodisc/res/pof/fund/index.html',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'
          }
client = MongoClient()
db = client.amac
collection_1 = db.fundinfo
collection_2 = db.managerinfo
dict_list = list()
for page in range(0, 1):
    rand = random.random()
    data = {}
    data = json.dumps(data)
    print("第%s页=====================" % str(page+1))
    url = 'https://gs.amac.org.cn/amac-infodisc/api/pof/fund?rand={rand}&page={page}&size=20'.format(rand=str(rand), page=str(page))
    response = requests.post(url, headers=headers, data=data, verify=False)
    try:
        datas = json.loads(response.text)["content"]
    except ValueError:
        try:
            datas = json.loads(response.text)["content"]
        except ValueError:
            datas = json.loads(response.text)["content"]
    count = 0
    fund_data_list = list()
    manager_data_list = list()
    for data in datas:
        count += 1
        print("正在爬取第" + str(count) + "条数据")
        time.sleep(1)
        jijinid = data['id']  # 基金ID
        managerurl = data['managerUrl']  # 经理页url
        fundName = data['fundName']  # 基金名称
        manager_name = data['managerName']  # 基金管理人名称
        url = data['url']
        fundurl = 'http://gs.amac.org.cn/amac-infodisc/res/pof/fund/' + url
        response = requests.get(fundurl, headers=headers, verify=False)
        html = response.content
        if html:
            soup = BeautifulSoup(html, 'lxml')
            table_list = soup.find_all('table', class_='table')
        else:
            response = requests.get(fundurl, headers=headers, verify=False)
            html = response.content
            soup = BeautifulSoup(html, 'lxml')
            table_list = soup.find_all('table', class_='table')
        fund_result = {}
        manager_result = {}
        #print(table_list)
        if table_list:
            for table in table_list:
                tr_list = table.find_all('tr')
                for tr in tr_list:
                    td_list = tr.find_all('td')
                    td_count = 1
                    key = ''
                    value = ''
                    for td in td_list:
                        if td_count == 1:
                            key = td.text.replace(" ", "").replace('\n', '')
                        elif td_count == 2:
                            value = td.text.replace(" ", "").replace('\n', '')
                        td_count += 1
                    #print({key: value})
                    fund_result.update({key: value})
            fund_data_list.append(fund_result)
        detmanagerurl = 'http://gs.amac.org.cn/amac-infodisc/res/pof/' + managerurl[3:]
        responsesecondinfo = requests.get(detmanagerurl, headers=headers, verify=False)
        html = responsesecondinfo.content
        if html:
            soup = BeautifulSoup(html, 'lxml')
            table_list = soup.find_all('table', class_='table')
        else:
            responsesecondinfo = requests.get(detmanagerurl, headers=headers, verify=False)
            html = responsesecondinfo.content
            soup = BeautifulSoup(html, 'lxml')
            table_list = soup.find_all('table', class_='table')
        if table_list:
            cnt = 1
            for table in table_list:
                tr_list = table.find_all('tr')
                if cnt == 1 or cnt == 4 or cnt == 5:
                    for tr in tr_list:
                        td_list = tr.find_all('td')
                        td_count = 1
                        key = ''
                        value = ''
                        for td in td_list:
                            if td_count == 1:
                                key = td.text.replace(" ", "").replace('\n', '')
                            elif td_count == 2:
                                value = td.text.replace(" ", "").replace('\n', '')
                            td_count += 1
                        #print({key: value})
                        manager_result.update({key: value})
                elif cnt == 2:
                    tr_count = 1
                    for tr in tr_list:
                        if tr_count == 1:
                            td_list = tr.find_all('td')
                            td_count = 1
                            key = ''
                            value = ''
                            for td in td_list:
                                if td_count == 1:
                                    key = td.text.replace(" ", "").replace('\n', '')
                                elif td_count == 2:
                                    if td.find('span', id='complaint1'):
                                        value = td.find('span', id='complaint1').text
                                    else:
                                        pass
                                    #print(td.find('span', id='complaint1').text)
                                td_count += 1
                            #print({key: value})
                            manager_result.update({key: value})
                        else:
                            td_list = tr.find_all('td')
                            td_count = 1
                            key = ''
                            value = ''
                            for td in td_list:
                                if td_count == 1:
                                    key = td.text.replace(" ", "").replace('\n', '')
                                elif td_count == 2:
                                    value = td.text.replace(" ", "").replace('\n', '')
                                td_count += 1
                            #print({key: value})
                            manager_result.update({key: value})
                        tr_count += 1
                elif cnt == 3:
                    for tr in tr_list:
                        td_list = tr.find_all('td')
                        td_count = 1
                        key = ''
                        value = ''
                        for td in td_list:
                            if td_count == 1 or td_count == 3:
                                key = td.text.replace(" ", "").replace('\n', '')
                            elif td_count == 2 or td_count == 4:
                                value = td.text.replace(" ", "").replace('\n', '')
                                #print({key: value})
                                manager_result.update({key: value})
                            td_count += 1
                    """
                elif cnt == 6:
                    tr_count = 1
                    for tr in tr_list:
                        if tr_count % 3 != 0:
                            td_list = tr.find_all('td')
                            td_count = 1
                            key = ''
                            value = ''
                            for td in td_list:
                                if td_count == 1 or td_count == 3:
                                    key = td.text.replace(" ", "").replace('\n', '')
                                elif td_count == 2 or td_count == 4:
                                    value = td.text.replace(" ", "").replace('\n', '')
                                    print({key: value})
                                    manager_result.update({key: value})
                                td_count += 1
                        else:
                            pass
                        tr_count += 1
                    """
                else:
                    pass
                cnt += 1
            manager_data_list.append(manager_result)
    count_fund = len(fund_data_list)
    collection_1.insert_many(fund_data_list)
    count_manager = len(manager_data_list)
    collection_2.insert_many(manager_data_list)
    cursor = collection_1.find({})
    fuseki = FusekiUpdate('http://localhost:3030', 'amac')
    g = Graph()
    for doc in cursor:
        fund = rp.FundProduct[doc['基金名称:']]
        RdfUtils.add_dict_to_graph(g, fund, {
            op.subordinateTo: doc['基金管理人名称:'],
            dp.createTime: doc['成立时间:']
        })
        fuseki.insert_graph(g, print_sparql=True, unsafe_auto_gen_type_rel=True)
    cursor = collection_2.find({})
    for doc in cursor:
        fund = rp.FundProduct[doc['基金名称:']]
        g.add((fund, RDF.type, rp.FundProduct.to_uri()))
        g.add(
            (fund, op.belongingBrand.value, rp.PrivateFundManager.val(doc['基金管理人名称:']))
        )
        fuseki.insert_graph(g)