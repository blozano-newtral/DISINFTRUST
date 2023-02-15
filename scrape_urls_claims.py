import argparse
import os
from multiprocessing import Pool
import multiprocessing
from typing import Dict, Any

import json
import requests
from tqdm import tqdm
from pymongo import MongoClient
from bson import json_util

n_cpus = multiprocessing.cpu_count()


def parse_url(url) -> Dict[str, Any]:
    base_url = "https://api.dev.newtral.es/extract/source"

    payload = json.dumps({"url": url, "extended_result": 0})
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.request(
        "POST", base_url, headers=headers, data=payload, timeout=60
    )

    return response.json()


def api_call(claim):
    monog_id = claim["_id"]

    filename = f"{args.save_dir}/{monog_id}.json"

    if os.path.isfile(filename):
        return

    url = claim["url"]

    try:
        parse = parse_url(url)
        claim["parse_url"] = parse

        with open(filename, "w") as file:
            file.write(json_util.dumps(claim))
    except Exception:
        print(f"Coudn't extract url {url} from claim {claim['_id']}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    # parser.add_argument("--sample", default=0, type=int)  # positional argument
    parser.add_argument("--save_dir", default="parse_claims_urls", type=str)
    # parser.add_argument("--file_path", default=None, type=str)
    args = parser.parse_args()

    if not os.path.exists(args.save_dir):
        print(f"Creating directory {args.save_dir}")
        os.makedirs(args.save_dir)

    client = MongoClient(
        "mongodb+srv://developers:Lu8Jv7SiweyPWL5u@clusternewtral.ybz85.mongodb.net/editor?retryWrites=true&w=majority"
    )
    db = client["claimreview"]["claim_reviews"]

    doc_count = db.count_documents({})
    results = db.find(filter={})  #%, batch_size=n_cpus)

    print(f"Scraping up to {doc_count} claims urls")
    with Pool(n_cpus) as pool:
        r = list(tqdm(pool.imap(api_call, results), total=doc_count))
