import requests
import mysql.connector
from datetime import datetime

# API 키 입력
API_KEY = "A73F3BA4D1A04658BAAFE34134E80CE9E0FE4655"

# API 엔드포인트
BASE_URL = {
    'kospi': 'http://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd',
    'kosdaq': 'http://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd',
    'konex': 'http://data-dbg.krx.co.kr/svc/apis/sto/knx_bydd_trd'
}

# MySQL 연결 설정
db_config = {
    'host': 'localhost',  # MySQL 서버 주소
    'user': 'pdc',        # MySQL 사용자 이름
    'password': '1234',  # MySQL 비밀번호
    'database': 'CJD'     # 데이터베이스 이름
}

# 데이터 요청 함수
def fetch_data(market, date):
    url = BASE_URL[market]
    headers = {'AUTH_KEY': API_KEY}
    params = {'basDd': date}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return data['OutBlock_1']  # 필요한 데이터 블록 반환
    else:
        print(f"Error fetching data for {market} on {date}: {response.status_code}")
        return []

# 데이터베이스에 데이터 저장 함수
def save_to_mysql(data, table_name):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # 테이블 생성 (없는 경우)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        basDd VARCHAR(8),
        isuCd VARCHAR(20),
        isuNm VARCHAR(100),
        market VARCHAR(10),
        tddOpnprc FLOAT,
        tddHgprc FLOAT,
        tddLwprc FLOAT,
        tddClsprc FLOAT,
        accTrdvol BIGINT,
        accTrdval BIGINT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # 데이터 삽입
    insert_query = f"""
    INSERT INTO {table_name} 
    (basDd, isuCd, isuNm, market, tddOpnprc, tddHgprc, tddLwprc, tddClsprc, accTrdvol, accTrdval)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for record in data:
        cursor.execute(insert_query, (
            record.get('basDd'), record.get('isuCd'), record.get('isuNm'), record.get('market'),
            record.get('tddOpnprc'), record.get('tddHgprc'), record.get('tddLwprc'), record.get('tddClsprc'),
            record.get('accTrdvol'), record.get('accTrdval')
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data saved to MySQL table {table_name}")

# 데이터 수집 및 저장
def collect_and_save_today_data_to_mysql(table_name):
    today_date = datetime.today().strftime('%Y%m%d')  # 현재 날짜를 'YYYYMMDD' 형식으로 가져옴
    all_data = []
    markets = ['kospi', 'kosdaq', 'konex']
    
    for market in markets:
        print(f"Fetching data for {market.upper()} on {today_date}...")
        data = fetch_data(market, today_date)
        if data:
            for record in data:
                record['market'] = market  # 시장 정보를 추가
                all_data.append(record)

    if all_data:
        save_to_mysql(all_data, table_name)

# 실행
if __name__ == "__main__":
    table_name = "krx_market_data"
    collect_and_save_today_data_to_mysql(table_name)

