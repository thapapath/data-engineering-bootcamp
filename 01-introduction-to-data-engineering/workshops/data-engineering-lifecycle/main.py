import json

import requests


# Get data from Dog API
# โค้ดส่วนนี้จะเป็นการเชื่อมต่อกับ Dog API โดยเราจะใส่ URL Endpoint
url = "https://dog.ceo/api/breeds/image/random"
# เสร็จแล้วจะยิง Request ไปดึงข้อมูลมาเก็บใส่ Response
response = requests.get(url)
data = response.json()
print(data)

# Write data to file
with open("dogs.json", "w") as f:
    json.dump(data, f)

# โค้ดส่วนนี้จะเป็นการเตรียมเรื่อง Credentials และ Configuration ต่าง ๆ เพื่อใช้ในการเชื่อมต่อกับ Dog API
api_url = "https://api.jsonbin.io/v3/b"
headers = {
    "Content-Type": "application/json",
    "X-Master-Key": "$2a$10$nE44UTK/uU6iNV0T4DIj3Orvmym0NLXS4XPpudGW/opICCPakrMy2",
    "X-Collection-Id": "65d05a77dc74654018a5f0a0",
}

# Read data from file
with open("dogs.json", "r") as f:
    data = json.load(f)

# โค้ดส่วนนี้จะเป็นการยิง Request ไปเพื่ออัพโหลดข้อมูลไปยังเซิฟเวอร์ของ JSONBin
response = requests.post(api_url, json=data, headers=headers)
print(response.json())