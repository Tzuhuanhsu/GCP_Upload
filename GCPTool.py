from concurrent.futures import ThreadPoolExecutor, as_completed

import getopt

import sys

import json

import os

import configparser

from google.cloud import storage

from google.oauth2 import service_account







#指令使用方式

# 建立 bucket

# python GCPTool.py -e Test/Release -b {bucketName} --createBucket

# 上傳檔案

# python GCPTool.py -e Test/Release -b {bucketName} --upload {資料夾位置}

class GCPTool():

    bucketName = None

    Config = None

    def __init__(self, param) -> None:

        try:

            opts, _ = getopt.getopt(param, "e:b:c upload:", ["bucket=", "createBucket","upload="])

        except:

            print("GCPTool param error")

            sys.exit()

        for opt , arg in opts:

            if opt in "-e":

                self.readConfig(arg)

            if opt in ("-b", "--bucket"):

                self.bucketName = arg

            elif opt in ("-c", "--createBucket"):

                self.createBucket()

            elif opt in("-upload","--upload"):

                print("upload")

                self.upload(arg)

    

    # 讀取對應的設定檔

    def readConfig(self, env)->None:

        print("red config", env)

        config = configparser.ConfigParser()

        config.read("Config.ini")

        self.Config = config[env]





    # 建立 Bucket

    def createBucket(self)-> None:

        with open(self.Config["TokenJSON"]) as source:

            info = json.load(source)

            storage_credentials = service_account.Credentials.from_service_account_info(info)  

            storage_client = storage.Client(project=self.Config["ProjectId"], credentials=storage_credentials)

            bucket = storage_client.create_bucket(

                bucket_or_name=self.bucketName,

                location="Asia",

                predefined_default_object_acl = "publicRead"

            )

            # set acl

            default_object_acl = bucket.default_object_acl

            default_object_acl.entity('allUsers').grant_read()

            default_object_acl.entity(f'project-owners-{self.Config["ProjectId"]}').grant_owner()

            default_object_acl.entity(f'project-editors-{self.Config["ProjectId"]}').grant_owner()

            default_object_acl.entity(f'project-viewers-{self.Config["ProjectId"]}').grant_read()

            default_object_acl.save()

            print(f'createBucket successfully >>{self.bucketName}')

    
    def getBucket(self, ):
        with open(self.Config["TokenJSON"]) as source:
            info = json.load(source)
            storage_credentials = service_account.Credentials.from_service_account_info(info)
            storage_client = storage.Client(project=self.Config["ProjectId"], credentials=storage_credentials)
            try:
                bucket = storage_client.get_bucket(self.bucketName)
                return bucket
            except Exception as e:
                print(f'bucket get_bucket exception: {e}')
        
    # 上傳檔案

    def upload(self, target)->None:

         max_workers = 10

         bucket = None

         with open(self.Config["TokenJSON"]) as source:

            info = json.load(source)

            storage_credentials = service_account.Credentials.from_service_account_info(info)  

            storage_client = storage.Client(project=self.Config["ProjectId"], credentials=storage_credentials)

            try:

                bucket = storage_client.get_bucket(self.bucketName)

            except Exception as e:

                 print(f'bucket get_bucket exception: {e}')

            with ThreadPoolExecutor(max_workers=max_workers) as executor:

                future_to_file = {}

                for root, _, files in os.walk(target):

                    for file_name in files:

                        local_file_path = os.path.join(root, file_name)

                        local_file_path = os.path.abspath(local_file_path)

                        blob_name = os.path.relpath(local_file_path, target).replace("\\", "/")

                        future = executor.submit(self.upload_file, bucket,  blob_name, local_file_path)

                        future_to_file[future] = local_file_path

        

      

                for future in as_completed(future_to_file):

                    try:

                        future.result()

                        print(f'File {future_to_file[future]} upload done..')

                    except Exception as e:

                        print(f'File {future_to_file[future]} generated an exception: {e}')

                print(f'upload successfully >> {self.bucketName}')

    def upload_file(self, bucket:storage.Bucket, blob_name, source_file_path):

        blob = bucket.blob(blob_name)

        blob.upload_from_filename(source_file_path)

        blob.cache_control = "no-cache" 

        blob.patch()

    def copy_blob(self, bucket, source_blob_name, destination_blob_name):
        try:
            source_blob = bucket.blob(source_blob_name)
            destination_blob = bucket.blob(destination_blob_name)
            destination_blob.rewrite(source_blob)
            print(f'Successfully copied {source_blob_name} to {destination_blob_name}')
        except Exception as e:
            print(f'Failed to copy {source_blob_name} to {destination_blob_name}: {e}')


    def copy_folder_within_bucket(self, bucketName,  source_folder, destination_folder, max_workers=5):
        print(self.Config["TokenJSON"])
        with open(self.Config["TokenJSON"]) as source:
            info = json.load(source)
            storage_credentials = service_account.Credentials.from_service_account_info(info)
            storage_client = storage.Client(project=self.Config["ProjectId"], credentials=storage_credentials)
            
            try:
                bucket = storage_client.get_bucket(bucketName)
            except Exception as e:
                print(f'bucket get_bucket exception: {e}')
                return

            blobs = bucket.list_blobs(prefix=source_folder)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_blob = {}
                for blob in blobs:
                    source_blob_name = blob.name
                    destination_blob_name = source_blob_name.replace(source_folder, destination_folder, 1)
                    print(f'Copying {source_blob_name} to {destination_blob_name}...')
                    future = executor.submit(self.copy_blob, bucket, source_blob_name, destination_blob_name)
                    future_to_blob[future] = source_blob_name

if __name__ == "__main__":
    patchModule = GCPTool(sys.argv[1:])

