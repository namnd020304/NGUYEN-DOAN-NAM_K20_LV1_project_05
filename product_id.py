import csv
import pymongo

URI = "mongodb://34.57.213.13:27017/"
def retrieve_product_id(URI):
    try:
        with pymongo.MongoClient(URI) as client:
            summary = client["glamira"]["summary"]
            product_ids = summary.distinct(
                "product_id",
                {"product_id": {"$exists": True, "$ne": None}}
            )
        return product_ids
    except ConnectionError as error:
        print(error)


def retrieve_product_id_fast(URI):
    try:
        with pymongo.MongoClient(URI) as client:
            summary = client["glamira"]["summary"]

            pipeline = [
                {"$match": {"product_id": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$product_id"}},
                {"$sort": {"_id": 1}}
            ]

            # allowDiskUse=True cho phép dùng disk khi RAM không đủ
            result = summary.aggregate(pipeline, allowDiskUse=True)
            product_ids = [doc["_id"] for doc in result]

        return product_ids
    except Exception as error:
        print(f"Error: {error}")
        return []

def load_to_csv(product_ids):
    with open('product_id.txt', mode='w') as file:
        for product_id in product_ids:
            file.write(str(product_id)+'\n')


if __name__ == "__main__":
    product_ids = retrieve_product_id_fast(URI)
    print(product_ids)
    load_to_csv(product_ids)
