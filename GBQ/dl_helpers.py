import random
import string
import os
import gzip 
import shutil
import glob

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor


def gen_random_sequence():
    return(''.join(random.choices(string.ascii_letters + string.digits, k=16)))


def extract_data_to_local_file(
        table,
        local_filename,
        client = None,
        temp_bucket = "dmurray_temp",
        random_seq = None
    ):
    """
    Extracts the contents of a GBQ table to a local file. 
    Requires that there exists a temporary bucket on Google 
    Cloud Storage to hold the files for transfer. 
    """

    if client is None:
      client = bigquery.Client()
    
    # Generate a temporary filename to hold the table...
    if random_seq == None:
        random_seq = gen_random_sequence()
    
    gcloud_tempfilename = f"temp_{random_seq}_*.csv.gz"

    destination_uri = "gs://{}/{}".format(temp_bucket, gcloud_tempfilename)

    # setup the job config
    job_config = bigquery.job.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP
    
    extract_job = client.extract_table(
        table,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config = job_config
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print("Export job completed...")


    #
    # Now extract the files from the google cloud bucket...
    #

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Get the bucket
    # Assume that one already exists to hold the results...
    bucket = storage_client.get_bucket("dmurray_temp")

    # Create a local directory to store the downloaded files
    local_dir = f".temp{random_seq}"
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    # Create the directory
    os.makedirs(local_dir)

    # List all blobs with the specified prefix
    blobs = list(bucket.list_blobs(prefix=f"temp_{random_seq}"))

    # Download all matching files in parallel
    def download_blob(blob):
        if blob.name.endswith(".csv.gz"):
            local_path = os.path.join(local_dir, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            print(f"Downloaded {blob.name}")
            blob.delete()
            print(f"Deleted {blob.name} from bucket")

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(download_blob, blobs)

    print("All files have been downloaded and removed from the bucket.")

    # Now unzip each of these compressed files
    print("Unzipping downloaded files...")
    for filename in os.listdir(local_dir):
        if filename.endswith(".csv.gz"):
            gz_path = os.path.join(local_dir, filename)
            csv_path = os.path.join(local_dir, filename[:-3])  # Remove .gz extension
            
            with gzip.open(gz_path, 'rb') as gz_file:
                with open(csv_path, 'wb') as csv_file:
                    csv_file.write(gz_file.read())
            
            # Remove the original .gz file
            os.remove(gz_path)

    #
    # Combine all .csv files into a single file...
    #
    allFiles = glob.glob(f"{local_dir}/*.csv")
    allFiles.sort()  # glob lacks reliable ordering, so impose your own if output order matters
    with open(local_filename, 'wb') as outfile:
        for i, fname in enumerate(allFiles):
            with open(fname, 'rb') as infile:
                if i != 0:
                    infile.readline()  # Throw away header on all but first file
                # Block copy rest of file from input to output without parsing
                shutil.copyfileobj(infile, outfile)
                print(fname + " has been imported.")
    
    # Delete the local directory that contained the individual .csv files
    shutil.rmtree(local_dir)
    print(f"Temporary directory {local_dir} has been deleted.")

    return(filename)