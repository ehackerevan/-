import yfinance as yf
import pandas as pd
import twstock
from datetime import datetime, timedelta
import logging
import numpy as np
import matplotlib.pyplot as plt

# 配置日誌
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler('stock_analysis_pipeline.log', encoding='utf-8'),
                        logging.StreamHandler()
                    ])

def get_taiwan_listed_stock_codes():
    """
    使用twstock獲取上市公司股票代碼
    """
    try:
        all_stocks = twstock.codes
        listed_stocks = [f"{code}.TW" for code, info in all_stocks.items() 
                         if code.isdigit() and len(code) == 4 and info.market == '上市']
        listed_stocks.append('^TWII')
        logging.info(f"成功獲取 {len(listed_stocks)-1} 支上市公司股票和 1 支指數")
        return listed_stocks
    except Exception as e:
        logging.error(f"獲取股票代碼時發生錯誤: {e}")
        return ['^TWII']

def fetch_stock_prices(stock_codes):
    """
    獲取近五年的收盤價數據
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 5)
    all_closing_prices = {}
    
    for stock_code in stock_codes:
        try:
            stock_data = yf.download(stock_code, start=start_date, end=end_date)
            if stock_data.empty:
                logging.warning(f"股票 {stock_code} 無法下載數據")
                continue
            closing_prices = stock_data['Close'].reset_index()
            closing_prices['Date'] = closing_prices['Date'].dt.tz_localize(None)
            closing_prices.columns = ['Date', stock_code]
            all_closing_prices[stock_code] = closing_prices
            logging.info(f"成功獲取 {stock_code} 的收盤價數據")
        except Exception as e:
            logging.error(f"股票 {stock_code} 下載失敗: {e}")
    return all_closing_prices

def merge_closing_prices(all_closing_prices):
    """
    合併所有股票的收盤價數據
    """
    merged_data = all_closing_prices['^TWII']
    for stock_code, prices in all_closing_prices.items():
        if stock_code != '^TWII':
            merged_data = pd.merge(merged_data, prices, on='Date', how='left')
    return merged_data

def analyze_stock_data(df):
    """
    分析台灣上市公司股價數據
    """
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    ma_windows = [20, 60, 120, 240]
    ma_columns = {}

    for column in df.columns:
        if column != '^TWII':
            for window in ma_windows:
                ma_columns[f'{column}_MA{window}'] = df[column].rolling(window=window).mean()

    ma_df = pd.DataFrame(ma_columns, index=df.index)
    combined_df = pd.concat([df, ma_df], axis=1)

    ma_above_count = {}
    for date in combined_df.index:
        daily_counts = {}
        for window in ma_windows:
            companies_above_ma = 0
            for column in combined_df.columns:
                if column != '^TWII' and column.endswith(f'_MA{window}'):
                    stock_column = column.replace(f'_MA{window}', '')
                    if pd.notna(combined_df.loc[date, column]) and combined_df.loc[date, stock_column] > combined_df.loc[date, column]:
                        companies_above_ma += 1
            daily_counts[f'Above_MA{window}_Count'] = companies_above_ma
        ma_above_count[date] = daily_counts

    ma_count_df = pd.DataFrame.from_dict(ma_above_count, orient='index')

    fig, ax1 = plt.subplots(figsize=(15, 10))
    for window in ma_windows:
        ax1.plot(ma_count_df.index, ma_count_df[f'Above_MA{window}_Count'], linestyle='--', label=f'Above MA{window}')
    ax1.set_xlabel('日期')
    ax1.set_ylabel('高於移動平均線的公司數量')
    ax1.set_ylim(0, 1000)
    ax1.legend(loc='upper left')

    ax2 = ax1.twinx()
    ax2.plot(combined_df.index, combined_df['^TWII'], color='red', linestyle='-', label='加權指數')
    ax2.set_ylabel('加權指數')
    ax2.set_ylim(10000, 26000)
    ax2.legend(loc='upper right')

    plt.title('台灣上市公司股價高於移動平均線的公司數量與加權指數')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('stock_ma_analysis.png')

    with pd.ExcelWriter('stock_analysis_results.xlsx') as writer:
        combined_df.to_excel(writer, sheet_name='原始數據及移動平均線')
        ma_count_df.to_excel(writer, sheet_name='移動平均線上漲公司數')

    logging.info("分析完成，結果已儲存為 'stock_analysis_results.xlsx' 和 'stock_ma_analysis.png'")
    return ma_count_df

def main():
    try:
        stock_codes = get_taiwan_listed_stock_codes()
        all_closing_prices = fetch_stock_prices(stock_codes)
        merged_data = merge_closing_prices(all_closing_prices)
        analysis_result = analyze_stock_data(merged_data)
        logging.info("完整流程執行成功")
    except Exception as e:
        logging.error(f"程序執行失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
