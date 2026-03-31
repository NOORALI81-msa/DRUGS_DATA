#!/usr/bin/env python3
"""Create practical MongoDB indexes for faster drug query retrieval."""
import os
from pymongo import MongoClient
from pymongo.errors import OperationFailure


def _normalize_index_keys(keys):
    return tuple((str(field), int(direction)) for field, direction in keys)


def ensure_indexes(collection):
    index_specs = [
        ([('spider_name', 1), ('site_domain', 1), ('run_number', 1), ('url', 1)], 'meta_spider_domain_run_url_idx'),
        ([('stored_at', -1)], 'stored_at_desc_idx'),
        ([('scraped_at', -1)], 'scraped_at_desc_idx'),
        ([('site_domain', 1), ('stored_at', -1)], 'site_domain_stored_at_idx'),
        ([('searchable_terms_lc', 1)], 'searchable_terms_lc_idx'),
        ([('brand_name_lc', 1), ('stored_at', -1)], 'brand_name_lc_stored_at_idx'),
        ([('drug_name_lc', 1), ('stored_at', -1)], 'drug_name_lc_stored_at_idx'),
        ([('generic_name_lc', 1), ('stored_at', -1)], 'generic_name_lc_stored_at_idx'),
        ([('salt_lc', 1), ('stored_at', -1)], 'salt_lc_stored_at_idx'),
        ([('title_lc', 1), ('stored_at', -1)], 'title_lc_stored_at_idx'),
        ([('data_brand_name_lc', 1), ('stored_at', -1)], 'data_brand_name_lc_stored_at_idx'),
        ([('data_drug_name_lc', 1), ('stored_at', -1)], 'data_drug_name_lc_stored_at_idx'),
        ([('data_generic_name_lc', 1), ('stored_at', -1)], 'data_generic_name_lc_stored_at_idx'),
        ([('data_salt_lc', 1), ('stored_at', -1)], 'data_salt_lc_stored_at_idx'),
        ([('data_title_lc', 1), ('stored_at', -1)], 'data_title_lc_stored_at_idx'),
    ]

    existing_indexes = collection.index_information()
    existing_key_map = {}
    for idx_name, idx_meta in existing_indexes.items():
        raw_keys = idx_meta.get('key', [])
        if not raw_keys:
            continue
        normalized = _normalize_index_keys(raw_keys)
        existing_key_map[normalized] = idx_name

    created = []
    skipped = []
    for keys, name in index_specs:
        normalized = _normalize_index_keys(keys)
        if normalized in existing_key_map:
            skipped.append((name, existing_key_map[normalized]))
            continue

        try:
            idx = collection.create_index(keys, name=name, background=True)
            created.append(idx)
        except OperationFailure as exc:
            # If another process created the same index concurrently, treat it as success.
            if getattr(exc, 'code', None) == 85:
                skipped.append((name, 'existing_conflict_name'))
                continue
            raise

    return created, skipped


def main():
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    mongo_db = os.getenv('MONGO_DATABASE', 'geometric_crawler')
    mongo_collection = os.getenv('MONGO_COLLECTION', 'spider_items')

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    collection = client[mongo_db][mongo_collection]

    created, skipped = ensure_indexes(collection)

    print(f'Connected: {mongo_uri}')
    print(f'Collection: {mongo_db}.{mongo_collection}')
    print('Indexes ensured:')
    for name in created:
        print(f'  - {name}')
    if skipped:
        print('Indexes already present (skipped):')
        for wanted_name, existing_name in skipped:
            print(f'  - wanted={wanted_name} existing={existing_name}')

    client.close()


if __name__ == '__main__':
    main()
