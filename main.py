import schedule, time, requests, datetime, pytz
import json
import pandas as pd
from pandas.io.json import json_normalize

urlheader = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36",
    "authority": "www.nseindia.com",
    "scheme":"https"
}

def save_data():
    symbol = 'BANKNIFTY'
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol
    data = requests.get(url, headers=urlheader).content
    data2 = data.decode('utf-8')
    df = json.loads(data2)

    current_time = datetime.datetime.now().strftime('%H:%M')
    total_oi_call = df["filtered"]["CE"]["totOI"]
    total_oi_put = df["filtered"]["PE"]["totOI"]
    total_volume_call = df["filtered"]["CE"]["totVol"]
    total_volume_put = df["filtered"]["PE"]["totVol"]
    oi_difference = total_oi_put - total_oi_call
    pcr = round(total_oi_put / total_oi_call, 2)

    file = json.load(open("data.json", 'r'))
    if file['date'] == str(datetime.date.today()):
        print("FOUND")
        # current_data = [current_time, total_oi_call, total_oi_put, total_volume_call, total_volume_put, oi_difference,
        #                 pcr]
        current_data = {
                        "current_time": current_time,
                        "total_oi_call": total_oi_call,
                        "total_oi_put": total_oi_put,
                        "total_volume_call": total_volume_call,
                        "total_volume_put": total_volume_put,
                        "oi_difference": oi_difference,
                        "pcr": pcr
                        }
        file['data'].append(current_data)
        file_to_write = open("data.json", 'w')
        json.dump(file, file_to_write)
        file_to_write.close()
        print("DATA UPDATED")
    else:
        file['date'] = str(datetime.date.today())
        file['data'].clear()
        file_to_write = open("data.json", 'w')
        json.dump(file, file_to_write)
        file_to_write.close()
        print("DATE ADDED")

def caller_save_data():
    start_time = datetime.datetime.strptime(convert_time_to_utc("09:10:00"), '%H:%M:%S').time()
    end_time = datetime.datetime.strptime(convert_time_to_utc("15:30:00"), '%H:%M:%S').time()

    while (start_time <= datetime.datetime.now().time() <= end_time):
        save_data()
        time.sleep(300)

    # while True:
    #     save_data()
    #     time.sleep(3)

def convert_time_to_utc(timerequest):
    local_timezone = pytz.timezone("Asia/Kolkata")
    # local_dt = datetime.datetime.utcnow().astimezone(local)
    local_time = str(datetime.datetime.now().strftime('%Y-%m-%d ')) + str(timerequest)
    naive = datetime.datetime.strptime(local_time, "%Y-%m-%d %H:%M:%S")
    local_dt = local_timezone.localize(naive)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime('%H:%M:%S')

schedule.every().monday.at(convert_time_to_utc("09:10:00")).do(caller_save_data)
schedule.every().tuesday.at(convert_time_to_utc("09:10:00")).do(caller_save_data)
schedule.every().wednesday.at(convert_time_to_utc("09:10:00")).do(caller_save_data)
schedule.every().thursday.at(convert_time_to_utc("09:10:00")).do(caller_save_data)
schedule.every().friday.at(convert_time_to_utc("09:10:00")).do(caller_save_data)

# caller_save_data()
# save_data()
while True:
    schedule.run_pending()
    time.sleep(60)