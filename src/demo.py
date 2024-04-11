import csv
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from dotenv import load_dotenv
import os


if __name__ == "__main__":
    load_dotenv('secrets/.env')
    username = os.getenv('USER_NAME')
    password = os.getenv('USER_PASSWORD')
    security_token = os.getenv('SECURITY_TOKEN')
    object_name = os.getenv('SALESFORCE_OBJECT_NAME')

    try:
        bulk = SalesforceBulk(
            username=username,
            password=password,
            security_token=security_token
        )
        print(f'Created job. Inserting to {object_name}...')
    except Exception as e:
        print(e)

    job = bulk.create_insert_job(
        object_name, # Make sure to provide API Name, not name you gave
        contentType='CSV',
        concurrency='Parallel'
    )

    reader = csv.DictReader(open('user.csv'))
    rows = [row for row in reader]
    csv_iter = CsvDictsAdapter(iter(rows))

    batch = bulk.post_batch(job, csv_iter) # do not use post_bulk_batch, it's broken
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)

    print(f"Done. Loaded {len(rows)} rows into {object_name}")