import requests
import json
import pandas as pd
import threading
import time

def auto_save():
    print("Autosave is enabled. Auto-save interval: 15 min")
    while True:
        time.sleep(15 * 60)
        request = requests.post(
                "http://localhost:80/admin",
                json = {"command" : "save all", "output" : ""}
                )
    
def handle_cmd():
    while True:
        cmd = input("Server command: ")

        request = requests.post(
                        "http://localhost:80/admin",
                        json = {"command" : cmd, "output" : ""}
                        )

        try:
            stamp_json = json.loads(
                        request
                        .json()["output"]
                        .replace("StampHistory ", "")
                        .replace("StampUserInfo ", "")
                        .replace('user_name','"user_name"')
                        .replace('user_id','"user_id"')
                        .replace('timestamp','"timestamp"')
                        .replace('stamp_history', '"stamp_history"')
                    )

            user_count:dict = {}

            for stamp_id,user_list in stamp_json["stamp_history"].items():
                for i in range(len(user_list)):
                    user_id = user_list[i]["user_id"]
                    try:
                        user_count[user_id] = {"count":user_count[user_id]["count"] + 1, "user_name" : user_list[i]["user_name"]}
                    except KeyError:
                        user_count[user_id] = {"count":1, "user_name" : user_list[i]["user_name"]}
                        
                        
            def remove_duplicates(data):
                # user_name을 기준으로 딕셔너리 정렬
                sorted_data = sorted(data.items(), key=lambda x: x[1]['user_name'])
                user_count = len(sorted_data)
                
                print(f"total user: {user_count}")

                
                # 중복된 user_name 제거
                unique_data = {}
                for key, value in sorted_data:
                    print(f"Users remaining until the duplicate user removal process is completed: {user_count}")
                    user_count -= 1
                    user_name = value['user_name']
                    count = value['count']

                    if user_name not in unique_data or count > unique_data[user_name]['count']:
                        unique_data[user_name] = {'count': count, 'user_name': user_name}

                # 최종 결과를 딕셔너리로 변환
                result_dict = {key: value for value in unique_data.values() for key, value in data.items() if value == unique_data[value['user_name']]}
                
                print(f"Total number of users with duplicates removed : {len(result_dict)}")

                return result_dict


            print("Converting json data to excel file.")
            df = pd.DataFrame(remove_duplicates(user_count).values())
            df.set_index('user_name', inplace=True)   #name 열을 인덱스로 지정

            df.to_excel("./stamp_user.xlsx")
        except:
            print(request.json()["output"])

a = threading.Thread(target=auto_save)
b = threading.Thread(target=handle_cmd)

a.start()
b.start()
a.join()
b.join()







    