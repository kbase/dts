from irods.session import iRODSSession
from irods.models import DataObject, DataObjectMeta
import boto3
import os

# Connect to iRODS
with iRODSSession(
    host='localhost',
    port=1247,
    user='rods',
    password='rods',
    zone='tempZone'
) as session:
  
    # List collections in the home directory
    home_path = f'/tempZone/home/{session.username}'
    collections = session.collections.get(home_path)
    print(f"Collections in {home_path}:")
    for coll in collections.subcollections:
        print(f" - {coll.id}:{coll.name}")

    # List data objects in the home directory
    print(f"\nData objects in {home_path}:")
    for obj in collections.data_objects:
        print(f" - {obj.id}:{obj.name}")

    # Create a new collection (first removing it and any files/metadata if they exists)
    new_coll_path = f'{home_path}/test-bucket'
    if session.collections.exists(new_coll_path):
        print(f'Collection {new_coll_path} already exists. Removing it for clean test.')
        try:
            coll = session.collections.get(new_coll_path)
            for obj in coll.data_objects:
                obj.unlink(force=True)
                print(f'Removed metadata from {obj.name}')
        except Exception as e:
            print(f'Error removing metadata: {e}')
        session.collections.remove(new_coll_path, recursive=True, force=True)
        print(f'Removed existing collection: {new_coll_path}')
    new_collection = session.collections.create(new_coll_path)


    # Upload multiple files to a new collection
    local_files = ['file1.txt', 'file2.txt', 'file3.txt']
    for filename in local_files:
        with open(filename, 'w') as f:
            f.write(f'This is {filename}\n')
        session.data_objects.put(filename, f'{new_coll_path}/{filename}')
        print(f'Uploaded {filename} to {new_coll_path}')
        os.remove(filename)

    # Verify upload
    uploaded_collection = session.collections.get(new_coll_path)
    print(f"\nData objects in {new_coll_path}:")
    for obj in uploaded_collection.data_objects:
        print(f" - {obj.name}")

    # Read file content
    for file in local_files:
        data_obj = session.data_objects.get(f'{new_coll_path}/{file}')
        with data_obj.open('r') as f:
            content = f.read()
            print(f'\nContent of {file}:')
            print(content)

    # Add metadata to a data objects
    for file in local_files:
        data_obj = session.data_objects.get(f'{new_coll_path}/{file}')
        data_obj.metadata.add('description', f'This is {file}')
        data_obj.metadata.add('author', 'M. Data')
        print(f'Added metadata to {file}')

    # Retrieve and print metadata
    for file in local_files:
        data_obj = session.data_objects.get(f'{new_coll_path}/{file}')
        metadata = data_obj.metadata['description']
        author = data_obj.metadata['author']
        print(f'Metadata for {file}: {metadata}; author: {author}')

    # Find a file by metadata
    print('\nFinding files with metadata description="This is file2.txt":')
    query = session.query(DataObject).filter(
        DataObjectMeta.name == 'description',
        DataObjectMeta.value == 'This is file2.txt'
    )
    for result in query.all():
        print(f' - Found: {result[DataObject.name]} in collection {result[DataObject.collection_id]}')

    # Now, try to access the files via S3 API
    s3_client = boto3.client(
        's3',
        use_ssl=False,
        endpoint_url='http://localhost:9010',
        aws_access_key_id='s3_access_key',
        aws_secret_access_key='s3_secret_key'
    )

    print(f"\nAccessing files in '{new_coll_path}' via S3 API:")
    list_bucket_result = s3_client.list_buckets()
    print("Buckets:")
    buckets = list_bucket_result.get("Buckets", [])
    for bucket in buckets:
        print(f" - {bucket['Name']}")

    for bucket in buckets:
        bucket_name = bucket['Name']
        print(f"\nObjects in bucket '{bucket_name}':")
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objects.get("Contents", []):
            print(f" - {obj['Key']}")
            obj_data = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            content = obj_data['Body'].read().decode('utf-8')
            print(f'   Content: {content}')


print("\nTest completed successfully.")