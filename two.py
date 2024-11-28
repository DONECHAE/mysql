import requests
import mysql.connector

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "user": "pdc",
    "password": "1234",
    "database": "CJD"
}

# Notion API 설정
notion_api_key = "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"
database_id = "14c957b2d884807d9c81f7087853dddb"

headers = {
    "Authorization": f"Bearer {notion_api_key}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# MySQL 데이터 가져오기
def fetch_mysql_data():
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor(dictionary=True)
    query = "SELECT basDd, isuCd, isuNm, market, tddOpnprc, tddHgprc, tddLwprc, tddClsprc, accTrdvol, accTrdval FROM KRX_DATA"
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()
    return results

# 노션에 데이터 삽입
def insert_into_notion(row):
    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": database_id},
        "properties": {
            "basDd": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["basDd"]
                        }
                    }
                ]
            },
            "isuCd": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["isuCd"]
                        }
                    }
                ]
            },
            "isuNm": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["isuNm"]
                        }
                    }
                ]
            },
            "market": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["market"]
                        }
                    }
                ]
            },
            "tddOpnprc": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["tddOpnprc"]
                        }
                    }
                ]
            },
            "tddHgprc": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["tddHgprc"]
                        }
                    }
                ]
            },
            "tddLwprc": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["tddLwprc"]
                        }
                    }
                ]
            },
            "tddClsprc": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["tddClsprc"]
                        }
                    }
                ]
            },
            "accTrdvol": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["accTrdvol"]
                        }
                    }
                ]
            },
            "accTrdval": {
                "rich_text": [
                    {
                        "text": {
                            "content": row["accTrdval"]
                        }
                    }
                ]
            }
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"Successfully added new row with isuCd: {row['isuCd']}")
    else:
        print(f"Failed to add new row with isuCd: {row['isuCd']}, Status Code: {response.status_code}, Error: {response.text}")

# 전체 데이터 업로드 실행
def fetch_and_upload_mysql_data():
    print("Uploading entire MySQL database to Notion...")
    data = fetch_mysql_data()
    for row in data:
        insert_into_notion(row)

# 전체 데이터 업로드 실행
fetch_and_upload_mysql_data()