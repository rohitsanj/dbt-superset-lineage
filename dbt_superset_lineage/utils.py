import json
import logging

from dbt_schemas.dbt_manifest_v9 import Model as DbtManifest, ModelNode


def get_datasets_from_superset(superset, superset_db_id):
    logging.info("Getting physical datasets from Superset.")

    page_number = 0
    datasets = []
    datasets_keys = set()
    while True:
        logging.info("Getting page %d.", page_number + 1)

        payload = {
            'q': json.dumps({
                'page': page_number,
                'page_size': 100
            })
        }
        res = superset.request('GET', '/dataset/', params=payload)

        result = res['result']
        if result:
            for r in result:
                kind = r['kind']
                database_id = r['database']['id']

                if kind == 'physical' \
                        and (superset_db_id is None or database_id == superset_db_id):

                    dataset_id = r['id']

                    name = r['table_name']
                    schema = r['schema']
                    dataset_key = f'{schema}.{name}'  # used as unique identifier

                    dataset_dict = {
                        'id': dataset_id,
                        'key': dataset_key
                    }

                    # fail if it breaks uniqueness constraint
                    assert dataset_key not in datasets_keys, \
                        f"Dataset {dataset_key} is a duplicate name (schema + table) " \
                        "across databases. " \
                        "This would result in incorrect matching between Superset and dbt. " \
                        "To fix this, remove duplicates or add the ``superset_db_id`` argument."

                    datasets_keys.add(dataset_key)
                    datasets.append(dataset_dict)
            page_number += 1
        else:
            break

    if not datasets:
        logging.warning("No datasets found!")

    return datasets


def get_tables_from_dbt(dbt_manifest: DbtManifest, dbt_db_name: str) -> dict[str, ModelNode]:
    """Based on manifest.json schema: https://schemas.getdbt.com/dbt/manifest/v9.json"""
    tables = {}
    for manifest_subset in [dbt_manifest.nodes, dbt_manifest.sources]:
        for table_key_long in manifest_subset:
            # Only look at dbt models
            if table_key_long.split(".")[0] not in ["model"]:
                continue
            table = manifest_subset[table_key_long]
            table_key_short = table.schema_ + '.' + table.name

            if dbt_db_name is None or table.database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, \
                    f"Table {table_key_short} is a duplicate name (schema + table) " \
                    f"across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."

                tables[table_key_short] = table

    assert tables, "Manifest is empty!"
    return tables
