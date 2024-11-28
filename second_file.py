import os
import requests
import mysql.connector
from datetime import datetime, timedelta

# 설정 정보
API_KEY = "A73F3BA4D1A04658BAAFE34134E80CE9E0FE4655"
BASE_URL = {
    'kospi': 'http://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd',
    'kosdaq': 'http://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd',
    'konex': 'http://data-dbg.krx.co.kr/svc/apis/sto/knx_bydd_trd'
}
db_config = {
    'host': 'localhost',
    'user': 'pdc',
    'password': '1234',
    'database': 'CJD'
}
state_file = "/home/one/mysql3/mysql/progress_second.txt"

def read_last_date():
    """상태 파일에서 마지막으로 처리한 날짜를 읽습니다."""
    if os.path.exists(state_file):
        with open(state_file, "r") as file:
            return file.read().strip()
    return None

def write_last_date(date):
    """마지막으로 처리한 날짜를 상태 파일에 저장합니다."""
    with open(state_file, "w") as file:
        file.write(date)

def fetch_data(market, date):
    """API에서 데이터를 가져옵니다."""
    url = BASE_URL[market]
    headers = {'AUTH_KEY': API_KEY}
    params = {'basDd': date}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('OutBlock_1', [])
        print(f"Fetched data for {market.upper()} on {date}: {len(data)} records")
        return data
    else:
        print(f"Error fetching data for {market.upper()} on {date}: {response.status_code}")
        return []

def save_to_mysql(data, market):
    """MySQL 데이터베이스에 데이터를 저장합니다."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS KRX_DATA (
        basDd TEXT,
        isuCd TEXT,
        isuNm TEXT,
        market TEXT,
        tddOpnprc TEXT,
        tddHgprc TEXT,
        tddLwprc TEXT,
        tddClsprc TEXT,
        accTrdvol TEXT,
        accTrdval TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    insert_query = """
    INSERT INTO KRX_DATA (
        basDd, isuCd, isuNm, market, tddOpnprc, tddHgprc, tddLwprc,
        tddClsprc, accTrdvol, accTrdval
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    field_mapping = {
        'BAS_DD': 'basDd',
        'ISU_CD': 'isuCd',
        'ISU_NM': 'isuNm',
        'TDD_OPNPRC': 'tddOpnprc',
        'TDD_HGPRC': 'tddHgprc',
        'TDD_LWPRC': 'tddLwprc',
        'TDD_CLSPRC': 'tddClsprc',
        'ACC_TRDVOL': 'accTrdvol',
        'ACC_TRDVAL': 'accTrdval'
    }
    
    for record in data:
        mapped_record = {field_mapping[key]: record.get(key) for key in field_mapping if key in record}
        mapped_record['market'] = market  # market 필드 추가
        if all(value not in [None, ""] for value in mapped_record.values()):
            cursor.execute(insert_query, (
                mapped_record.get('basDd'), mapped_record.get('isuCd'), mapped_record.get('isuNm'),
                mapped_record.get('market'), mapped_record.get('tddOpnprc'), mapped_record.get('tddHgprc'),
                mapped_record.get('tddLwprc'), mapped_record.get('tddClsprc'), mapped_record.get('accTrdvol'),
                mapped_record.get('accTrdval')
            ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data saved to MySQL table KRX_DATA: {len(data)} records")

def collect_data():
    """어제 데이터를 수집합니다."""
    yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    last_date = read_last_date()
    
    if last_date == yesterday_date:
        print("Data already processed for yesterday.")
        return
    
    for market in ['kospi', 'kosdaq', 'konex']:
        print(f"Fetching data for {market.upper()} on {yesterday_date}...")
        data = fetch_data(market, yesterday_date)
        if data:
            save_to_mysql(data, market)
        else:
            print(f"No data fetched for {market.upper()} on {yesterday_date}. Skipping.")
    
    if any(fetch_data(market, yesterday_date) for market in ['kospi', 'kosdaq', 'konex']):
        write_last_date(yesterday_date)
        print("Yesterday's data processed and saved.")
    else:
        print("No data available for any market. Nothing to process.")

if __name__ == "__main__":
    collect_data()
