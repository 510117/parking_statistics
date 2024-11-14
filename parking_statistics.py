import os
import re
import datetime
import pandas as pd
from tqdm import tqdm
from itertools import product

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
    df['進入日'] = pd.to_datetime(df['進入日'], errors='coerce')  # 用 'coerce' 將無法轉換的值設為 NaT
    df['進入時間'] = pd.to_datetime(df['進入時間'], errors='coerce', format='%H:%M:%S').dt.time
    df['出場日'] = pd.to_datetime(df['出場日'], errors='coerce')
    df['出場時間'] = pd.to_datetime(df['出場時間'], errors='coerce', format='%H:%M:%S').dt.time

    # time stamp
    df['enter_ts'] = pd.to_datetime(df['進入日'].astype(str) + ' ' + df['進入時間'].astype(str), format="%Y-%m-%d %H:%M:%S", errors='coerce')
    df['leave_ts'] = pd.to_datetime(df['出場日'].astype(str) + ' ' + df['出場時間'].astype(str), format="%Y-%m-%d %H:%M:%S", errors='coerce')

    df['停留時數'] = (df['leave_ts'] - df['enter_ts']).dt.total_seconds() / 3600
    df['停留時數'] = df['停留時數'].fillna(0)

    return df

def filter_by_time(df, start_date, end_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # 篩選進入時間和出場時間在指定日期範圍內
    df = df[(df['出場日'] >= start_date) & (df['進入日'] <= end_date)]
    df.reset_index(drop=True, inplace=True)

    # print(df['enter_ts'].head(10))
    # print(df['leave_ts'].head(10))
    
    return df


def analyze_parking_data(file_path, categories, start_date, end_date):
    df = load_data(file_path)
    df = filter_by_time(df, start_date, end_date)
    return df

def generate_average_max_vehicles(df, categories, start_date, end_date):
    print("開始分析每小時同時停留在校園內的平均最高車輛數 (1/4)")
    time_bins = pd.date_range("00:00", "23:59", freq="h").time
    week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    columns = [cat + "_" + day for day, cat in product(week_days, categories)]
    weekly_avg_max_vehicles = pd.DataFrame(index = time_bins, columns = columns)

    size_table = {}
    for i in range(len(week_days)):
        for time_bin in time_bins:
            for cat in categories:
                size_table[(i, time_bin, cat)] = []
    
    current_date = start_date
    with tqdm(total = (end_date - start_date).days) as pbar:
        while current_date <= end_date:
            for time_bin in time_bins:
                for cat in categories:
                    start_time = current_date.replace(hour = time_bin.hour)
                    end_time = current_date.replace(hour = time_bin.hour, minute = 59)
                    vehicles_in_time_bin = df[(df['enter_ts'] <= end_time) & (df['leave_ts'] >= start_time) & (df['票種'] == cat)]
                    size_table[(current_date.weekday(), time_bin, cat)].append(len(vehicles_in_time_bin))
            current_date += datetime.timedelta(days = 1)
            pbar.update(1)
            # print(current_date)

    for i in range(len(week_days)):
        for time_bin in time_bins:
            for cat in categories:
                weekly_avg_max_vehicles.loc[time_bin, (cat + "_" + week_days[i])] = list_mean(size_table[(i, time_bin, cat)])
    return weekly_avg_max_vehicles
def generate_max_vehicles_in_period(df, time_periods, categories, start_date, end_date):
    print("開始分析某時段內同時停留在校園內的最高車輛數 (2/4)")
    time_periods_str = [start_time.strftime("%H:%M:%S") + "-" + end_time.strftime("%H:%M:%S") for (start_time, end_time) in time_periods]
    week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    columns = [cat + "_" + day for day, cat in product(week_days, categories)]
    avg_max_vehicles_in_period = pd.DataFrame(index = time_periods_str, columns = columns)

    current_date = start_date
    size_table = {}
    for i in range(len(week_days)):
        for j in range(len(time_periods)):
            for cat in categories:
                size_table[(i, time_periods_str[j], cat)] = []

    with tqdm(total = (end_date - start_date).days) as pbar:
        while current_date <= end_date:
            for j in range(len(time_periods)):
                for cat in categories:
                    start_time = current_date.replace(hour = time_periods[j][0].hour, minute = time_periods[j][0].minute)
                    if time_periods[j][1].hour == 24:
                        end_time = current_date.replace(hour = 0, minute = time_periods[j][0].minute)
                        end_time += datetime.timedelta(days = 1)
                    else:
                        end_time = current_date.replace(hour = time_periods[j][1].hour, minute = time_periods[j][1].minute)
                    vehicles_in_time_bin = df[(df['enter_ts'] <= end_time) & (df['leave_ts'] >= start_time) & (df['票種'] == cat)]
                    start_str = time_periods[j][0].strftime("%H:%M:%S")
                    end_str = time_periods[j][1].strftime("%H:%M:%S")
                    size_table[(current_date.weekday(), start_str + "-" + end_str, cat)].append(len(vehicles_in_time_bin))
            current_date += datetime.timedelta(days = 1)
            pbar.update(1)

    for i in range(len(week_days)):
        for j in range(len(time_periods)):
            for cat in categories:
                start_str = time_periods[j][0].strftime("%H:%M:%S")
                end_str = time_periods[j][1].strftime("%H:%M:%S")
                avg_max_vehicles_in_period.loc[time_periods_str[j], (cat + "_" + week_days[i])] = list_mean(size_table[(i, time_periods_str[j], cat)])
    return avg_max_vehicles_in_period

def generate_vehicle_in_out_by_hour(df, categories, start_date, end_date):
    print("開始分析在該小時內進出校園的車輛數 (3/4)")
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
    
    with tqdm(total = (end_date - start_date).days) as pbar:

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
            pbar.update(1)

    for i in range(len(week_days)):
        for typ in INOUT:
            for time_bin in time_bins:
                for cat in categories:
                    columns_str = cat + "_" + week_days[i] + "_" + typ
                    vehicle_in_out_by_hour.loc[time_bin, columns_str] = list_mean(size_table[(i, typ, time_bin, cat)])
    return vehicle_in_out_by_hour

def generate_longest_continuous_stay(df, categories):
    print("開始分析最高連續停留時長統計 (4/4)")
    time_bin = [744, 696, 648, 600, 552, 504, 456, 408, 360, 312, 264, 216, 168, 144, 120, 96, 72, 48, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 1, 0.5, 0]
    time_bin_str = ['744 (31days)', '696 (29days)', '648 (27days)', '600 (25days)', '552 (23days)', '504 (21days)', '456 (19days)', '408 (17days)', '360 (15days)', '312 (13days)', '264 (11days)', '216 (9 days)', '168 (7days)', '144 (6days)', '120 (5days)', '96 (4days)', '72 (3days)', '48 (2days)', '24 (1day)', '22', '20', '18', '16', '14', '12', '10', '8', '6', '4', '2', '1', '0.5', '0']
    longest_stays = pd.DataFrame(columns = categories, index = time_bin_str)
    size_table = {}
    for i in range(len(time_bin)):
        for cat in categories:
            size_table[(i, cat)] = 0
            
    with tqdm(total = len(df)) as pbar:
        for index, row in df.iterrows():
            i = 0
            while(i < len(time_bin) - 1 and time_bin[i] > row['停留時數']):
                # print(time_bin[i + 1], row['停留時數'])
                i += 1
            size_table[(i, row['票種'])] += 1
            pbar.update(1)

    for i in range(len(time_bin)):
        for cat in categories:
            longest_stays.loc[time_bin_str[i], cat] = size_table[(i, cat)]
    
    return longest_stays

def save_to_excel(arena, df, tables):
    # columns_to_include = ['車號', '票種', '子場站', '進站設備', '進入時間', '出場時間', '停留時數', 'enter_ts', 'leave_ts']
    # df_filtered = df[columns_to_include]
    # df_filtered.columns = [col.strip() for col in df_filtered.columns]

    with pd.ExcelWriter(arena + '校區統計結果.xlsx', engine='xlsxwriter') as writer:
        tables[0].to_excel(writer, sheet_name='Avg Max Vehicles')
        tables[1].to_excel(writer, sheet_name='Max Vehicles in Period')
        tables[2].to_excel(writer, sheet_name='Vehicle In_Out by Hour')
        tables[3].to_excel(writer, sheet_name='Longest Continuous Stay')
        # df_filtered.to_excel(writer, sheet_name='Parking Data', index=False)


def is_valid_datetime(input_str, date_format= "%Y-%m-%d %H:%M:%S"):
    try:
        datetime.datetime.strptime(input_str, date_format)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    start_date = input("請輸入欲查詢開始日期 (ex. 2024-10-1): ")
    if not is_valid_datetime(start_date, "%Y-%m-%d"):
        print("輸入起始日期的格式不對")
        print("錯誤輸入: " + start_date)
        exit(1)
    start_date = pd.to_datetime(start_date)


    end_date = input("請輸入欲查詢結束日期 (ex. 2024-10-30): ")
    if not is_valid_datetime(end_date, "%Y-%m-%d"):
        print("輸入結束日期的格式不對")
        print("錯誤輸入: " + end_date)
        exit(1)

    end_date = pd.to_datetime(end_date)

    time_periods_input = list(input("請輸入要查詢的時間點，可多個，請用空白隔開\n若不須查詢特定時間，可以直接按enter跳過 (ex. 8:00-17:00 8:00-13:00): ").split(' '))
    time_periods_input = [period.strip() for period in time_periods_input if period.strip()]

    time_periods = []
    for time_period_input in time_periods_input:
        try:
            start_oclock, end_oclock = time_period_input.split("-")
        except (ValueError):
            print("輸入時間範圍的格式不對")
            print("錯誤輸入: " + time_period_input)
            exit(1)
        if not is_valid_datetime(start_oclock, "%H:%M"):
            print("輸入起始時間點的格式不對")
            print("錯誤輸入: " + start_oclock)
            exit(1)

        if not is_valid_datetime(end_oclock, "%H:%M"):
            print("輸入結束時間點的格式不對")
            print("錯誤輸入: " + end_oclock)
            exit(1)
            
        time_periods.append((start_oclock, end_oclock))

    # print(time_periods)
    for i in range(len(time_periods)):
        start_oclock = datetime.datetime.strptime(time_periods[i][0], "%H:%M").time()
        end_oclock = datetime.datetime.strptime(time_periods[i][1], "%H:%M").time()
        time_periods[i] = [start_oclock, end_oclock]

    print("開始分析")
    
    for arena in ['光復', '博愛']:
        categories = []
        folder_path = arena + '校區資料夾'
        pattern = re.compile(r".*" + arena + ".*\.xlsx$")
        files_path = [f for f in os.listdir(folder_path) if pattern.match(f)]
        # print(files_path)
        print("讀取" + arena + "校區停車資料中")
        df = pd.DataFrame()
        for file_path in files_path:
            df_file = analyze_parking_data(folder_path + "/" + file_path, categories, start_date, end_date)
            df = pd.concat([df, pd.DataFrame(df_file)])
        # print(df)
        print("分析" + arena + "校區停車資料中")
        avg_max_vehicles = generate_average_max_vehicles(df, categories, start_date, end_date)
        max_vehicles_in_period = generate_max_vehicles_in_period(df, time_periods, categories, start_date, end_date)
        vehicle_in_out_by_hour = generate_vehicle_in_out_by_hour(df, categories, start_date, end_date)
        longest_continuous_stay = generate_longest_continuous_stay(df, categories)
        
        tables = [avg_max_vehicles, max_vehicles_in_period, vehicle_in_out_by_hour, longest_continuous_stay]
        
        save_to_excel(arena, df, tables)
