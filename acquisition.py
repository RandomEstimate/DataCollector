# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 23:19:57 2019

@author: asus
"""
import datetime, pytz
from threading import Thread
import requests
import time
from queue import Queue
import pandas as pd
import ccxt
import os
import csv
def get_time_info(name_list,symbol_list):		
    time_dict = {}
    for name in name_list:
        for symbol in symbol_list:
            if os.path.exists(name + '/' + symbol.replace('/','').lower() + '.csv'):
            
                file = open(name + '/' + symbol.replace('/','').lower() + '.csv', 'a+', newline='')
                file.seek(0, 0)
                reader = csv.reader(file, delimiter=' ')
                tmp_list = [i for i in reader]
                file.close()
                
                if tmp_list != 0:
                    time_dict.update({
                            name + symbol:tmp_list[len(tmp_list) - 1][0].split(',')[0]
                            })
                else:
                    time_dict.update({
                            name + symbol:'1374744100000'
                            })
               
                
            else:
                
                time_dict.update({
                            name + symbol:'1374744100000'
                            })
    return time_dict


class CrawlInfo(Thread):

    def __init__(self, obj, info_queue, exchange):

        Thread.__init__(self)
        self.obj = obj
        self.req = requests.session()
        self.info_queue = info_queue
        self.exchange = exchange
        self.symbol_list = ['BTC/USDT', 'ETH/USDT','EOS/USDT','LTC/USDT','BCH/USDT','ETC/USDT','XRP/USDT']

    def run(self):
        while 1:
            try:          
                for symbol in self.symbol_list:
                    self.info_queue.put({'exchange': self.exchange,
                                         'symbol': symbol,
                                         'text': self.obj.fetch_ohlcv(symbol,'1m')})
                    time.sleep(1)
                time.sleep(60)
            except:
                pass

class Parse(Thread):
    def __init__(self, info_queue):
        global time_info
        Thread.__init__(self)
        self.info_queue = info_queue            
        
    def timestamp2iso(self,timestamp, format='%Y-%m-%dT%H:%M:%S.%fZ'):
      
        format = format.replace('%f', '{-FF-}')  # 订单处理微秒数据 %f
        length = min(16, len(str(timestamp)))  # 最多去到微秒级

        # 获取毫秒/微秒 数据
        sec = '0'
        if length != 10:  # 非秒级
            sec = str(timestamp)[:16][-(length - 10):]  # 最长截取16位长度 再取最后毫秒/微秒数据
        sec = '{:0<6}'.format(sec)  # 长度位6，靠左剩下的用0补齐
        timestamp = float(str(timestamp)[:10])  # 转换为秒级时间戳
        return datetime.datetime.utcfromtimestamp(timestamp).strftime(format).replace('{-FF-}', sec)
		
    def run(self):
        
        while 1:
            if self.info_queue.empty() == False:

                tmp = self.info_queue.get()

                exchange = tmp['exchange']
                
                symbol = tmp['symbol']

                data = tmp['text']

                data = pd.DataFrame(data)

                data.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                
                data = data[:-1]
                
                data = data[data['time'].astype(int) > int(time_info[exchange + symbol])]
                
    
                if not data.empty:
                    
                    data = data.sort_values(by='time').reset_index(drop=True)
                    
                    data['timestamp'] = data.time.apply(lambda x: self.timestamp2iso(str(x)[:-3]))
                
                    time_info.update({
                            exchange + symbol : data['time'][len(data)-1]
                            })
                    
                    file = open(exchange + '/' + symbol.replace('/','').lower() + '.csv', 'a+', newline='')
                    write = csv.writer(file, dialect='excel')
                    for i in range(len(data)):
                        write.writerow(data.iloc[i])
                    file.close()                
					
                time.sleep(1.5)


if __name__ == '__main__':
	
    
    obj_list = [ccxt.okex(),ccxt.huobipro(),ccxt.binance(),ccxt.bitmax(),ccxt.fcoin(),ccxt.upbit()]
    name_list = ['okex','huobipro','binance','bitmax','fcoin','upbit']
    symbol_list = ['BTC/USDT', 'ETH/USDT','EOS/USDT','LTC/USDT','BCH/USDT','ETC/USDT','XRP/USDT']
    time_info = get_time_info(name_list,symbol_list)
    
    info_queue = Queue()
	
    for obj, name in zip(obj_list, name_list):
        crawl = CrawlInfo(obj, info_queue,name)
        crawl.start()
    for i in range(5):
        Parse_thread = Parse(info_queue)
        Parse_thread.start()
    Parse_thread.join()
