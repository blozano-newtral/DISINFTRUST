import argparse
import os
from multiprocessing import Pool
import multiprocessing
from typing import Dict, Any

import json
import requests
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient
from bson import json_util

n_cpus = multiprocessing.cpu_count()

OVERWRITE = False

def parse_url(url) -> Dict[str, Any]:
    base_url = "https://api.dev.newtral.es/extract/source"

    payload = json.dumps({"url": url, "extended_result": 0})
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.request(
        "POST", base_url, headers=headers, data=payload, timeout=60
    )

    return response.json()


def api_call(claim):
    mongo_id, url = claim

    filename = f"{args.save_dir}/{mongo_id}.json"

    if not OVERWRITE and os.path.isfile(filename):
        return

    try:
        parse = parse_url(url)

        with open(filename, "w") as file:
            file.write(json_util.dumps(parse))
    except Exception:
        print(f"Coudn't extract url {url} from claim {mongo_id}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--save_dir", default="parse_claims_urls", type=str)
    parser.add_argument("--csv_file", default="claim_urls.csv", type=str)
    parser.add_argument("--overwrite", action='store_false')
    
    args = parser.parse_args()

    OVERWRITE = args.overwrite

    if not os.path.exists(args.save_dir):
        print(f"Creating directory {args.save_dir}")
        os.makedirs(args.save_dir)

    df = pd.read_csv(args.csv_file)
    print(f"Scraping up to {len(df)} claims urls")

    call_arguments = [tuple(claim) for index, claim in df.iterrows()]
    with Pool(n_cpus) as pool:
        r = list(tqdm(pool.imap(api_call, call_arguments), total=len(call_arguments)))
