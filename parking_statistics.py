import pandas as pd
import os
import re
from itertools import product
import datetime


def in_time(t_start, t_end, t):
    return t_start <= t and t <= t_end

def list_mean(L):
    if len(L) == 0:
        return 0
    return sum(L) / len(L)

def load_data(file_path):
    # 讀取資料並跳過第一行
    df = pd.read_excel(file_path, skiprows=1)
    
    # 篩選出偶數行 (0, 2, 4, 6, ...)
    df = df.iloc[::2].reset_index(drop=True)
    for index, row in df.iterrows():
        if row['票種'] not in categories:
            categories.append(row['票種'])
    # 檢查 '進入日' 和 '進入時間' 是否有空值或格式錯誤
    df['進入日'] = pd.to_datetime(df['進入日'], errors='coerce')  # 用 'coerce' 將無法轉換的值設為 NaT
    df['進入時間'] = pd.to_datetime(df['進入時間'], errors='coerce', format='%H:%M:%S').dt.time
    df['出場日'] = pd.to_datetime(df['出場日'], errors='coerce')
    df['出場時間'] = pd.to_datetime(df['出場時間'], errors='coerce', format='%H:%M:%S').dt.time
    # 組合 '進入日' 和 '進入時間'、'出場日' 和 '出場時間' 為 datetime

    # time stamp
    df['enter_ts'] = pd.to_datetime(df['進入日'].astype(str) + ' ' + df['進入時間'].astype(str), errors='coerce')
    df['leave_ts'] = pd.to_datetime(df['出場日'].astype(str) + ' ' + df['出場時間'].astype(str), errors='coerce')

    df['停留時數'] = (df['leave_ts'] - df['enter_ts']).dt.total_seconds() / 3600
    df['停留時數'] = df['停留時數'].fillna(0)

    return df

def filter_by_time(df, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # 轉換 '進入時間' 和 '出場時間' 的時間部分為 datetime.time 類型
    # 篩選進入時間和出場時間在指定日期範圍內，且在指定時間區間內
    df = df[(df['出場日'] >= start_date) & (df['進入日'] <= end_date)]
    df.reset_index(drop=True, inplace=True)

    # 顯示轉換後的時間欄位，檢查結果
    # print(df['enter_ts'].head(10))
    # print(df['leave_ts'].head(10))
    
    return df


def analyze_parking_data(file_path, categories, start_date, end_date):
    df = load_data(file_path)
    df = filter_by_time(df, start_date, end_date)
    return df

def generate_average_max_vehicles(df, categories, start_date, end_date):
    # 更新此處，'H' 改為 'h'，避免 deprecated 警告
    time_bins = pd.date_range("00:00", "23:59", freq="h").time
    week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    columns = [cat + "_" + day for cat, day in product(categories, week_days)]
    weekly_avg_max_vehicles = pd.DataFrame(index = time_bins, columns = columns)

    current_date = start_date
    size_table = {}
    for i in range(len(week_days)):
        for time_bin in time_bins:
            for cat in categories:
                size_table[(i, time_bin, cat)] = []
    while current_date <= end_date:
        for time_bin in time_bins:
            for cat in categories:
                start_time = current_date.replace(hour = time_bin.hour)
                end_time = current_date.replace(hour = time_bin.hour, minute = 59)
                vehicles_in_time_bin = df[(df['enter_ts'] <= end_time) & (df['leave_ts'] >= start_time) & (df['票種'] == cat)]
                size_table[(current_date.weekday(), time_bin, cat)].append(len(vehicles_in_time_bin))
        current_date += datetime.timedelta(days = 1)
    
    for i in range(len(week_days)):
        for time_bin in time_bins:
            for cat in categories:
                weekly_avg_max_vehicles.loc[time_bin, (cat + "_" + week_days[i])] = list_mean(size_table[(i, time_bin, cat)])
    return weekly_avg_max_vehicles
def generate_max_vehicles_in_period(df, time_periods, categories, start_date, end_date):
    # 更新此處，'H' 改為 'h'，避免 deprecated 警告
    time_periods_str = [start_time.strftime("%H:%M:%S") + "-" + end_time.strftime("%H:%M:%S") for (start_time, end_time) in time_periods]
    week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    columns = [cat + "_" + day for cat, day in product(categories, week_days)]
    avg_max_vehicles_in_period = pd.DataFrame(index = time_periods_str, columns = columns)

    current_date = start_date
    size_table = {}
    for i in range(len(week_days)):
        for j in range(len(time_periods)):
            for cat in categories:
                size_table[(i, time_periods_str[j], cat)] = []
    while current_date <= end_date:
        for j in range(len(time_periods)):
            for cat in categories:
                start_time = current_date.replace(hour = time_periods[j][0].hour, minute = time_periods[j][0].minute, second = time_periods[j][0].second)
                if time_periods[j][1].hour == 24:
                    end_time = current_date.replace(hour = 0, minute = time_periods[j][0].minute, second = time_periods[j][0].second)
                    end_time += datetime.timedelta(days = 1)
                else:
                    end_time = current_date.replace(hour = time_periods[j][1].hour, minute = time_periods[j][1].minute, second = time_periods[j][0].second)
                vehicles_in_time_bin = df[(df['enter_ts'] <= end_time) & (df['leave_ts'] >= start_time) & (df['票種'] == cat)]
                start_str = time_periods[j][0].strftime("%H:%M:%S")
                end_str = time_periods[j][1].strftime("%H:%M:%S")
                size_table[(current_date.weekday(), start_str + "-" + end_str, cat)].append(len(vehicles_in_time_bin))
        current_date += datetime.timedelta(days = 1)
    
    for i in range(len(week_days)):
        for j in range(len(time_periods)):
            for cat in categories:
                start_str = time_periods[j][0].strftime("%H:%M:%S")
                end_str = time_periods[j][1].strftime("%H:%M:%S")
                avg_max_vehicles_in_period.loc[time_periods_str[j], (cat + "_" + week_days[i])] = list_mean(size_table[(i, time_periods_str[j], cat)])
    return avg_max_vehicles_in_period

def generate_vehicle_in_out_by_hour(df, categories, start_date, end_date):
    time_bins = pd.date_range("00:00", "23:59", freq="h").time
    INOUT = ["In", "Out"]
    week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    columns = [cat + "_" + day + "_" + typ for typ, day, cat in product(INOUT, week_days, categories)]
    vehicle_in_out_by_hour = pd.DataFrame(index = time_bins, columns = columns)

    current_date = start_date
    size_table = {}
    for i in range(len(week_days)):
        for typ in INOUT:
            for time_bin in time_bins:
                for cat in categories:
                    size_table[(i, typ, time_bin, cat)] = []
    while current_date <= end_date:
        for time_bin in time_bins:
            for cat in categories:
                for typ in INOUT:
                    start_time = current_date.replace(hour = time_bin.hour)
                    end_time = current_date.replace(hour = time_bin.hour, minute = 59)
                    if typ == "In":
                        vehicles_in_time_bin = df[(df['enter_ts'] <= end_time) & (df['enter_ts'] >= start_time) & (df['票種'] == cat)]
                    elif type == "Out":
                        vehicles_in_time_bin = df[(df['leave_ts'] <= end_time) & (df['leave_ts'] >= start_time) & (df['票種'] == cat)]
                    size_table[(current_date.weekday(), typ, time_bin, cat)].append(len(vehicles_in_time_bin))
        current_date += datetime.timedelta(days = 1)
    
    for i in range(len(week_days)):
        for typ in INOUT:
            for time_bin in time_bins:
                for cat in categories:
                    columns_str = cat + "_" + week_days[i] + "_" + typ
                    vehicle_in_out_by_hour.loc[time_bin, columns_str] = list_mean(size_table[(i, typ, time_bin, cat)])
    return vehicle_in_out_by_hour

def generate_longest_continuous_stay(df, categories):
    time_bin = [744, 696, 648, 600, 552, 504, 456, 408, 360, 312, 264, 216, 168, 144, 120, 96, 72, 48, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 1, 0.5, 0]
    time_bin_str = ['744 (31days)', '696 (29days)', '648 (27days)', '600 (25days)', '552 (23days)', '504 (21days)', '456 (19days)', '408 (17days)', '360 (15days)', '312 (13days)', '264 (11days)', '216 (9 days)', '168 (7days)', '144 (6days)', '120 (5days)', '96 (4days)', '72 (3days)', '48 (2days)', '24 (1day)', '22', '20', '18', '16', '14', '12', '10', '8', '6', '4', '2', '1', '0.5', '0']
    longest_stays = pd.DataFrame(columns = categories, index = time_bin_str)
    size_table = {}
    for i in range(len(time_bin)):
        for cat in categories:
            size_table[(i, cat)] = 0
            
    for index, row in df.iterrows():
            i = 0
            while(i < len(time_bin) - 1 and time_bin[i] > row['停留時數']):
                # print(time_bin[i + 1], row['停留時數'])
                i += 1
            size_table[(i, row['票種'])] += 1
    
    for i in range(len(time_bin)):
        for cat in categories:
            longest_stays.loc[time_bin_str[i], cat] = size_table[(i, cat)]
    
    return longest_stays

def save_to_excel(arena, df, tables):
    # 只選擇需要的欄位
    columns_to_include = ['車號', '票種', '子場站', '進站設備', 
                          '進入時間', '出場時間', '停留時數', 'enter_ts', 'leave_ts']
    df_filtered = df[columns_to_include]
    
    # 移除欄位名稱中的空白
    df_filtered.columns = [col.strip() for col in df_filtered.columns]

    # 寫入 Excel
    with pd.ExcelWriter(arena + '校區統計結果.xlsx', engine='xlsxwriter') as writer:
        tables[0].to_excel(writer, sheet_name='Avg Max Vehicles')
        tables[1].to_excel(writer, sheet_name='Max Vehicles in Period')
        tables[2].to_excel(writer, sheet_name='Vehicle In_Out by Hour')
        tables[3].to_excel(writer, sheet_name='Longest Continuous Stay')
        df_filtered.to_excel(writer, sheet_name='Parking Data', index=False)

if __name__ == "__main__":
    start_date = input("請輸入欲查詢開始日期 (ex. 2024-10-1): ")
    start_date = pd.to_datetime(start_date)

    end_date = input("請輸入欲查詢結束日期 (ex. 2024-10-30): ")
    end_date = pd.to_datetime(end_date)

    folder_path = '停車統計資料夾'
    time_periods_input = list(input("請輸入要查詢的時間點，可多個，請用空白隔開 (ex. 08:00-17:00 08:00-13:00): ").split(' '))
    time_periods = []
    for time_period_input in time_periods_input:
        start_oclock, end_oclock = time_period_input.split("-")
        start_oclock += ":00"
        end_oclock += ":00"
        time_periods.append((start_oclock, end_oclock))
    print(time_periods)
    for i in range(len(time_periods)):
        start_oclock = datetime.datetime.strptime(time_periods[i][0], "%H:%M:%S").time()
        end_oclock = datetime.datetime.strptime(time_periods[i][1], "%H:%M:%S").time()
        time_periods[i] = [start_oclock, end_oclock]

    for arena in ['光復', '博愛']:
        categories = []

        # 定義正則表達式的模式，例如僅選擇以 "data_" 開頭、以 ".txt" 結尾的檔案
        pattern = re.compile(r".*" + arena + ".*\.xlsx$")
        # 取得符合模式的檔案列表
        files_path = [f for f in os.listdir(folder_path) if pattern.match(f)]
        print(files_path)

        df = pd.DataFrame()
        for file_path in files_path:
            df_file = analyze_parking_data(folder_path + "/" + file_path, categories, start_date, end_date)
            df = pd.concat([df, pd.DataFrame(df_file)])
        # print(df)
        avg_max_vehicles = generate_average_max_vehicles(df, categories, start_date, end_date)
        max_vehicles_in_period = generate_max_vehicles_in_period(df, time_periods, categories, start_date, end_date)
        vehicle_in_out_by_hour = generate_vehicle_in_out_by_hour(df, categories, start_date, end_date)
        longest_continuous_stay = generate_longest_continuous_stay(df, categories)
        
        tables = [avg_max_vehicles, max_vehicles_in_period, vehicle_in_out_by_hour, longest_continuous_stay]
        
        save_to_excel(arena, df, tables)
