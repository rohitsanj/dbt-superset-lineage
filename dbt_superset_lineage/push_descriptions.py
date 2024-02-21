import logging
import re
import time

from bs4 import BeautifulSoup
from markdown import markdown
from requests import HTTPError

from .superset_api import Superset
from dbt_superset_lineage.dbt_schemas.dbt_manifest_v9 import Model as DbtManifest, ModelNode
from .utils import get_datasets_from_superset, get_tables_from_dbt
import threading

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


def refresh_columns_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request('PUT', f'/dataset/{dataset_id}/refresh')


def add_superset_columns(superset, dataset):
    logging.info("Pulling fresh columns info from Superset.")

    res = superset.request('GET', f"/dataset/{dataset['id']}")
    result = res['result']

    dataset['columns'] = result['columns']
    dataset['description'] = result['description']
    dataset['owners'] = result['owners']

    return dataset


def convert_markdown_to_plain_text(md_string):
    """Converts a markdown string to plaintext.

    The following solution is used:
    https://gist.github.com/lorey/eb15a7f3338f959a78cc3661fbc255fe
    """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(md_string)

    # remove code snippets
    html = re.sub(r'<pre>(.*?)</pre>', ' ', html)
    html = re.sub(r'<code>(.*?)</code >', ' ', html)

    # extract text
    soup = BeautifulSoup(html, 'html.parser')
    text = ''.join(soup.findAll(text=True))

    # make one line
    single_line = re.sub(r'\s+', ' ', text)

    # make fixes
    single_line = re.sub('→', '->', single_line)
    single_line = re.sub('<null>', '"null"', single_line)

    return single_line


def merge_columns_info(dataset, tables: dict[str, ModelNode]):
    logging.info("Merging columns info from Superset and manifest.json file.")

    key = dataset['key']
    sst_columns = dataset['columns']
    dbt_columns = {}

    sst_description = dataset['description']
    dbt_description = None

    if key in tables:
        dbt_columns = tables[key].columns
        dbt_description = tables[key].description

    sst_owners = dataset['owners']

    columns_new = []
    for sst_column in sst_columns:

        column_name = sst_column['column_name']

        # add the mandatory fields
        column_new = {
            'column_name': column_name,
            'id': sst_column['id']
        }

        # add column descriptions
        if column_name in dbt_columns and (
            sst_column['expression'] is None
            or sst_column['expression'] == ''
        ):
            description = dbt_columns[column_name].description
            description = convert_markdown_to_plain_text(description)
        else:
            description = sst_column['description']
        column_new['description'] = description

        columns_new.append(column_new)

    dataset['columns_new'] = columns_new

    # add dataset description
    if dbt_description is None:
        dataset['description_new'] = sst_description
    else:
        dataset['description_new'] = convert_markdown_to_plain_text(dbt_description)

    # add dataset owner IDs (otherwise Superset empties the owners list)
    dataset['owners_new'] = [owner['id'] for owner in sst_owners]

    return dataset


def check_columns_equal(lst1, lst2):
    return sorted(lst1, key=lambda c: c["id"]) == sorted(lst2, key=lambda c: c["id"])


def pause_after_update(superset_pause_after_update):
    if superset_pause_after_update:
        logging.info("Pausing the script for %d seconds to allow for databases to catch up with the update.",
                     superset_pause_after_update)
        time.sleep(superset_pause_after_update)
        logging.info("Resuming the script again.")


def put_descriptions_to_superset(superset, dataset, superset_pause_after_update):
    logging.info("Putting model and column descriptions into Superset.")

    description_new = dataset['description_new']
    columns_new = dataset['columns_new']
    owners_new = dataset['owners_new']

    description_old = dataset['description']
    columns_old = [{
        'column_name': col['column_name'],
        'id': col['id'],
        'description': col['description']
    } for col in dataset['columns']]

    if description_new != description_old or \
       not check_columns_equal(columns_new, columns_old):
        payload = {'description': description_new, 'columns': columns_new, 'owners': owners_new}
        superset.request('PUT', f"/dataset/{dataset['id']}?override_columns=false", json=payload)
        pause_after_update(superset_pause_after_update)
    else:
        logging.info("Skipping PUT execute request as nothing would be updated.")


def create_dataset(
        superset: Superset, superset_db_id: int, dbt_table: ModelNode,
):
    # Get or create from Superset
    # Schema: https://github.com/apache/superset/blob/master/superset/datasets/schemas.py#L270-L284
    logging.info(f"Starting to create dataset for dbt table '{dbt_table.name}'")
    resp: dict = superset.request("POST", "/dataset/get_or_create", json={
        "database_id": superset_db_id,
        "schema": dbt_table.schema_,
        "table_name": dbt_table.name,
    })
    table_id = resp.get("result", {}).get("table_id")
    logging.info(f"Created dataset in Superset with id {table_id}")
    assert table_id, "No table_id returned, something's broken!"


def create_datasets_from_dbt_tables(dbt_tables, superset, superset_db_id):
    sst_datasets = get_datasets_from_superset(superset, superset_db_id)
    logging.info("There are %d physical datasets in Superset overall.", len(sst_datasets))
    sst_dataset_keys = set(d["key"] for d in sst_datasets)
    dbt_tables_not_in_superset: list[ModelNode] = [
        val for key, val in dbt_tables.items() if key not in sst_dataset_keys
    ]
    logging.info(f"About to create {len(dbt_tables_not_in_superset)} datasets in Superset")
    create_dataset_threads = [
        threading.Thread(target=create_dataset, args=(superset, superset_db_id, dbt_table), )
        for dbt_table in dbt_tables_not_in_superset
    ]
    # Start all the threads
    for thread in create_dataset_threads:
        thread.start()
    # Wait for all threads to complete
    for thread in create_dataset_threads:
        thread.join()
    logging.info("Successfully created datasets from dbt models")


def main(dbt_project_dir, dbt_db_name,
         superset_url, superset_db_id, superset_refresh_columns, superset_pause_after_update,
         superset_access_token, superset_refresh_token, create_dataset_if_not_exists=False):

    # require at least one token for Superset
    assert superset_access_token is not None or superset_refresh_token is not None, \
           "Add ``SUPERSET_ACCESS_TOKEN`` or ``SUPERSET_REFRESH_TOKEN`` " \
           "to your environment variables or provide in CLI " \
           "via ``superset-access-token`` or ``superset-refresh-token``."

    superset = Superset(superset_url + '/api/v1',
                        access_token=superset_access_token, refresh_token=superset_refresh_token)

    logging.info("Starting the script!")
    dbt_manifest = DbtManifest.parse_file(f'{dbt_project_dir}/target/manifest.json')

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)

    if create_dataset_if_not_exists:
        create_datasets_from_dbt_tables(dbt_tables, superset, superset_db_id)

    # Fetch again after creating
    sst_datasets = get_datasets_from_superset(superset, superset_db_id)

    sst_datasets_dbt_filtered = [d for d in sst_datasets if d["key"] in dbt_tables]
    logging.info("There are %d physical datasets in Superset with a match in dbt.", len(sst_datasets_dbt_filtered))

    for i, sst_dataset in enumerate(sst_datasets_dbt_filtered):
        logging.info("Processing dataset %d/%d.", i + 1, len(sst_datasets_dbt_filtered))
        sst_dataset_id = sst_dataset['id']
        try:
            if superset_refresh_columns:
                refresh_columns_in_superset(superset, sst_dataset_id)
                pause_after_update(superset_pause_after_update)
            sst_dataset_w_cols = add_superset_columns(superset, sst_dataset)
            sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables)
            put_descriptions_to_superset(superset, sst_dataset_w_cols_new, superset_pause_after_update)
        except HTTPError as e:
            logging.error("The dataset with ID=%d wasn't updated. Check the error below.",
                          sst_dataset_id, exc_info=e)

    logging.info("All done!")

