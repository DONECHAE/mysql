import os
import requests
import mysql.connector
from datetime import datetime, timedelta
import logging
import subprocess  # 크론 작업 삭제를 위해 추가

# 로깅 설정
LOG_FILE = "/home/one/mysql3/mysql/process.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# API 키 및 URL 설정
API_KEY = "A73F3BA4D1A04658BAAFE34134E80CE9E0FE4655"
BASE_URL = {
    'kospi': 'http://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd',
    'kosdaq': 'http://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd',
    'konex': 'http://data-dbg.krx.co.kr/svc/apis/sto/knx_bydd_trd'
}

# MySQL 연결 설정
db_config = {
    'host': 'localhost',
    'user': 'pdc',
    'password': '1234',
    'database': 'CJD'
}

# 상태 파일 경로
state_file = "/home/one/mysql3/mysql/progress_state.txt"

# 시작 및 끝 날짜 설정
START_DATE = datetime(2010, 8, 16)
END_DATE = datetime(2024, 11, 24)

# 필드 매핑
field_mapping = {
    'BAS_DD': 'basDd',
    'ISU_CD': 'isuCd',
    'ISU_NM': 'isuNm',
    'MKT_NM': 'market',
    'TDD_OPNPRC': 'tddOpnprc',
    'TDD_HGPRC': 'tddHgprc',
    'TDD_LWPRC': 'tddLwprc',
    'TDD_CLSPRC': 'tddClsprc',
    'ACC_TRDVOL': 'accTrdvol',
    'ACC_TRDVAL': 'accTrdval'
}

def read_last_state():
    """상태 파일에서 마지막으로 처리한 연도를 읽음."""
    if os.path.exists(state_file):
        with open(state_file, "r") as file:
            return int(file.read().strip())
    return START_DATE.year  # 기본 시작 연도

def write_last_state(year):
    """처리한 마지막 연도를 상태 파일에 저장."""
    with open(state_file, "w") as file:
        file.write(str(year))
    logging.info(f"Saved last processed year: {year}")

def fetch_data(market, date):
    """API에서 데이터를 가져옴."""
    url = BASE_URL[market]
    headers = {'AUTH_KEY': API_KEY}
    params = {'basDd': date}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json().get('OutBlock_1', [])
    else:
        logging.error(f"Error fetching data for {market.upper()} on {date}: {response.status_code}")
        return []

def save_to_mysql(data, market_name):
    """MySQL 데이터베이스에 데이터를 저장."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
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
    """)
    conn.commit()

    insert_query = """
    INSERT INTO KRX_DATA (
        basDd, isuCd, isuNm, market, tddOpnprc, tddHgprc, tddLwprc, tddClsprc, accTrdvol, accTrdval
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for record in data:
        mapped_record = {field_mapping[key]: record[key] for key in field_mapping if key in record}
        mapped_record['market'] = market_name  # 마켓 정보 추가
        if all(value not in [None, ""] for value in mapped_record.values()):  # 값이 None이거나 빈 문자열이 아닌 경우에만 저장
            cursor.execute(insert_query, (
                mapped_record.get('basDd'), mapped_record.get('isuCd'), mapped_record.get('isuNm'),
                mapped_record.get('market'), mapped_record.get('tddOpnprc'), mapped_record.get('tddHgprc'),
                mapped_record.get('tddLwprc'), mapped_record.get('tddClsprc'), mapped_record.get('accTrdvol'),
                mapped_record.get('accTrdval')
            ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Saved data to MySQL table: KRX_DATA")

def collect_data_by_year(year):
    """지정된 연도의 데이터를 수집."""
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    # 시작 날짜 조정
    if year == START_DATE.year:
        start_date = START_DATE
    if year == END_DATE.year:
        end_date = END_DATE

    for market in ['kospi', 'kosdaq', 'konex']:
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            try:
                data = fetch_data(market, date_str)
                if data:
                    save_to_mysql(data, market)
                else:
                    logging.info(f"No data for {market.upper()} on {date_str}")
            except Exception as e:
                logging.error(f"Error processing {market.upper()} on {date_str}: {e}")
            current_date += timedelta(days=1)

def remove_cron_job():
    """크론 작업 삭제."""
    try:
        # `process.log`를 기준으로 특정 크론 작업 제거
        cron_comment = "/home/one/mysql3/mysql/process.log"
        subprocess.run(f"crontab -l | grep -v '{cron_comment}' | crontab -", shell=True, check=True)
        logging.info("Cron job removed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to remove cron job: {e}")

def main():
    last_processed_year = read_last_state()

    if last_processed_year > END_DATE.year:
        logging.info("All data already processed.")
        return

    next_year_to_process = last_processed_year + 1
    logging.info(f"Starting data collection for year: {next_year_to_process}")
    collect_data_by_year(next_year_to_process)
    write_last_state(next_year_to_process)

    if next_year_to_process >= END_DATE.year:
        logging.info("End date reached. Removing cron job.")
        remove_cron_job()

if __name__ == "__main__":
    main()
