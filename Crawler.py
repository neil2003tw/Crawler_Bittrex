import pandas, numpy
import codecs, csv, os
import urllib3, certifi, json
from datetime import timedelta,datetime
#from bs4 import BeautifulSoup

#Adjustable variable
Time_var=30
First_use=True

#Proper convert for variable
Time_str={30:'thirtyMin'}
Time_var_str=Time_str[Time_var]

## Crawler setup
http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs=certifi.where()
    ) # Verifies certificates when making requests

## Prepare data storage for Bittrex market
Bitweb_Marketlist = pandas.DataFrame(columns=('CurrencyName',
                                              'CurrencyShort',
                                              'Market',
                                              'TradeCurrency',
                                              'Starttime'))
## Retrieving data from json
response=http.request('GET','https://bittrex.com/Api/v2.0/pub/Markets/GetMarkets')
raw_string=codecs.decode(response.data, 'utf-8')
json_dict=json.loads(raw_string)

for i,market in enumerate(json_dict['result']):
    Bitweb_Marketlist.loc[i]=[market['MarketCurrencyLong'],
                              market['MarketCurrency'],
                              market['MarketName'],
                              market['BaseCurrency'],
                              market['Created']]
    print('Yoho, ' + market['MarketCurrency'] + ' is done!')

if First_use:
    Bitweb_Marketlist.to_csv('Bittrex_marketintro.csv')

## Create Time Interval
S_Times=list(pandas.to_datetime(Bitweb_Marketlist['Starttime']))
S_Time_dt = datetime.strptime(str(min(S_Times)),'%Y-%m-%d %H:%M:%S')
dtn=datetime.now()
dtn=dtn.replace(minute=0, second=0, microsecond=0)
Time_count=( dtn- S_Time_dt )//timedelta(minutes=30)
Time_list=[]
Time_list = [str(dtn-(timedelta(minutes=Time_var)*(i+1))).replace(' ','_') for i in range(0,Time_count)]

## Create Dataframes for data
Bittrex_open = pandas.DataFrame(columns=Time_list,index=Bitweb_Marketlist['Market']+'_'+Bitweb_Marketlist['TradeCurrency'])
Bittrex_close = Bittrex_open
Bittrex_high = Bittrex_open
Bittrex_low = Bittrex_open
Bittrex_volume = Bittrex_open
Bittrex_btcvalue = Bittrex_open

#list_pages=['https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName='+
#            x + '&tickInterval=' + Time_var_str for x in list(Bitweb_Marketlist['Market'])]
market_list=list(Bitweb_Marketlist['Market'])
tradeC_list=list(Bitweb_Marketlist['TradeCurrency'])

## Retriving Data from different JSON
for i in range(0, 1):
    url='https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=' +\
        market_list[i] + '&tickInterval=' + Time_var_str
    response=http.request('GET',url)
    raw_string=codecs.decode(response.data, 'utf-8')
    json_dict=json.loads(raw_string)
    ## Arrange data to proper location
    for perdata in json_dict['result']:
        Dperdata=perdata['T'].replace('T','_')
        Nperdata=market_list[i]+'_' +tradeC_list[i]
        Bittrex_open[Dperdata].loc[Nperdata]=perdata['O']
        Bittrex_close[Dperdata].loc[Nperdata] = perdata['C']
        Bittrex_high[Dperdata].loc[Nperdata] = perdata['H']
        Bittrex_low[Dperdata].loc[Nperdata] = perdata['L']
        Bittrex_volume[Dperdata].loc[Nperdata] = perdata['V']
        Bittrex_btcvalue[Dperdata].loc[Nperdata] = perdata['BV']
    print("Chao, " + Nperdata + " is done!")

if not os.path.exists('RetrievedData'):
    os.makedirs('RetrievedData')

Bittrex_open.to_csv('RetrievedData/Bittrex_open.csv', sep='\t', encoding='utf-8')
Bittrex_close.to_csv('RetrievedData/Bittrex_close.csv', sep='\t', encoding='utf-8')
Bittrex_high.to_csv('RetrievedData/Bittrex_high.csv', sep='\t', encoding='utf-8')
Bittrex_low.to_csv('RetrievedData/Bittrex_low.csv', sep='\t', encoding='utf-8')
Bittrex_volume.to_csv('RetrievedData/Bittrex_volume.csv', sep='\t', encoding='utf-8')
Bittrex_btcvalue.to_csv('RetrievedData/Bittrex_btcvalue.csv', sep='\t', encoding='utf-8')


