import requests
import json
import time
from datetime import datetime, timedelta
import os


class BinanceKlineDownloader:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3/klines"
        self.request_delay = 0.1  # 防止请求过快被限制
        
    def timestamp_to_milliseconds(self, date_str):
        """将日期字符串转换为毫秒时间戳"""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.timestamp() * 1000)
    
    def get_kline_data(self, symbol, interval, start_time, end_time, limit=1000):
        """获取K线数据"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None
    
    def format_kline_data(self, kline_data):
        """格式化K线数据为易读格式"""
        formatted_data = []
        for kline in kline_data:
            formatted_line = {
                'open_time': datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'open_price': kline[1],
                'high_price': kline[2],
                'low_price': kline[3],
                'close_price': kline[4],
                'volume': kline[5],
                'close_time': datetime.fromtimestamp(kline[6] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                'quote_asset_volume': kline[7],
                'number_of_trades': kline[8],
                'taker_buy_base_asset_volume': kline[9],
                'taker_buy_quote_asset_volume': kline[10]
            }
            formatted_data.append(formatted_line)
        return formatted_data
    
    def save_to_txt(self, data, filename):
        """保存数据到txt文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            # 写入表头
            f.write("开盘时间\t开盘价\t最高价\t最低价\t收盘价\t成交量\t收盘时间\t成交额\t成交笔数\t主动买入成交量\t主动买入成交额\n")
            
            # 写入数据
            for item in data:
                line = f"{item['open_time']}\t{item['open_price']}\t{item['high_price']}\t{item['low_price']}\t{item['close_price']}\t{item['volume']}\t{item['close_time']}\t{item['quote_asset_volume']}\t{item['number_of_trades']}\t{item['taker_buy_base_asset_volume']}\t{item['taker_buy_quote_asset_volume']}\n"
                f.write(line)
    
    def download_historical_data(self, symbol, start_date, end_date, interval='1h'):
        """下载历史K线数据"""
        print(f"开始下载 {symbol} 的历史K线数据...")
        print(f"时间范围: {start_date} 到 {end_date}")
        print(f"时间间隔: {interval}")
        
        start_timestamp = self.timestamp_to_milliseconds(start_date)
        end_timestamp = self.timestamp_to_milliseconds(end_date)
        
        all_data = []
        current_start = start_timestamp
        
        # 创建保存目录
        if not os.path.exists('kline_data'):
            os.makedirs('kline_data')
        
        while current_start < end_timestamp:
            # 计算当前批次的结束时间（最多1000条记录）
            # 1小时间隔，1000条记录约为41.67天
            batch_end = min(current_start + (1000 * 60 * 60 * 1000), end_timestamp)
            
            print(f"正在下载: {datetime.fromtimestamp(current_start/1000).strftime('%Y-%m-%d %H:%M:%S')} 到 {datetime.fromtimestamp(batch_end/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 获取数据
            data = self.get_kline_data(symbol, interval, current_start, batch_end)
            
            if data:
                all_data.extend(data)
                print(f"成功获取 {len(data)} 条记录")
            else:
                print("获取数据失败，跳过此批次")
            
            # 更新下一批次的开始时间
            current_start = batch_end
            
            # 添加延迟以避免触发API限制
            time.sleep(self.request_delay)
        
        # 格式化数据
        formatted_data = self.format_kline_data(all_data)
        
        # 保存到文件
        filename = f"kline_data/{symbol}_{interval}_{start_date}_to_{end_date}.txt"
        self.save_to_txt(formatted_data, filename)
        
        print(f"数据下载完成！总共获取 {len(formatted_data)} 条记录")
        print(f"文件保存位置: {filename}")
        
        return filename


def main():
    # 初始化下载器
    downloader = BinanceKlineDownloader()
    
    # 设置参数
    symbol = "BTCUSDT"  # 可以修改为其他交易对
    start_date = "2020-01-01"
    end_date = "2025-07-01"
    interval = "1h"  # 1小时K线
    
    # 下载数据
    try:
        filename = downloader.download_historical_data(symbol, start_date, end_date, interval)
        print(f"\n下载完成！数据已保存到: {filename}")
    except Exception as e:
        print(f"下载过程中发生错误: {e}")


if __name__ == "__main__":
    main()