from pymongo import MongoClient
client = MongoClient("mongodb://34.57.245.3:27017/")

try:
    # start example code here
    # end example code here
    client.admin.command("ping")
    print("Connected successfully")
    # other application code
    summary = client['glamira']['summary']
    pipeline = [
        {"$group": {"_id": "$ip"}},
        {"$project": {"_id": 0, "ip": "$_id"}}
    ]
    distinct_ips = []
    for doc in summary.aggregate(pipeline, allowDiskUse=True):
        distinct_ips.append(doc['ip'])

    print(f"Tổng số IP unique: {len(distinct_ips)}")
    client.close()
except Exception as e:
    raise Exception(
        "The following error occurred: ", e)



