import requests
import mysql.connector

# MySQL 연결 설정
db_config = {
    "host": "localhost",
    "user": "your_mysql_user",
    "password": "your_mysql_password",
    "database": "your_database"
}

# Notion API 설정
notion_api_key = "YOUR_NOTION_API_KEY"
database_id = "YOUR_NOTION_DATABASE_ID"
headers = {
    "Authorization": f"Bearer {notion_api_key}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# MySQL 데이터 가져오기
def fetch_mysql_data():
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor(dictionary=True)
    query = "SELECT basDd, isuCd, isuNm, market, tddOpnprc, tddHgprc, tddLwprc, tddClsprc, accTrdvol, accTrdval, last_updated FROM KRX_DATA"
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()
    return results

# 노션 데이터 가져오기
def fetch_notion_data():
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        data = response.json()["results"]
        notion_data = []
        for item in data:
            properties = item["properties"]
            notion_data.append({
                "page_id": item["id"],
                "isuCd": properties["isuCd"]["rich_text"][0]["text"]["content"],
                "last_updated": properties["Last Updated"]["date"]["start"] if properties["Last Updated"]["date"] else None
            })
        return notion_data
    else:
        print(f"Failed to fetch Notion data, Status Code: {response.status_code}, Error: {response.text}")
        return []

# 노션 페이지 업데이트
def update_notion_page(page_id, row):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
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
            },
            "Last Updated": {
                "date": {
                    "start": row["last_updated"].isoformat() if row["last_updated"] else None
                }
            }
        }
    }
    response = requests.patch(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"Successfully updated row with isuCd: {row['isuCd']}")
    else:
        print(f"Failed to update row with isuCd: {row['isuCd']}, Status Code: {response.status_code}, Error: {response.text}")

# 노션에 새로운 페이지 추가
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

# 변경된 데이터 추가 및 업데이트 실행
def job():
    print("Checking for updates...")

    # MySQL 및 노션 데이터 가져오기
    mysql_data = fetch_mysql_data()
    notion_data = fetch_notion_data()

    # MySQL 데이터와 노션 데이터 비교
    mysql_data_dict = {row["isuCd"]: row for row in mysql_data}
    notion_data_dict = {row["isuCd"]: row for row in notion_data}

    # 데이터 추가 또는 수정 처리
    for isuCd, mysql_row in mysql_data_dict.items():
        if isuCd not in notion_data_dict:
            # 노션에 없는 데이터 -> 추가
            insert_into_notion(mysql_row)
        else:
            # 노션에 있는 데이터 -> 수정 여부 확인
            notion_row = notion_data_dict[isuCd]
            if mysql_row["last_updated"].isoformat() != notion_row["last_updated"]:
                update_notion_page(notion_row["page_id"], mysql_row)

# 변경 사항을 즉시 확인하고 업데이트 실행
job()
