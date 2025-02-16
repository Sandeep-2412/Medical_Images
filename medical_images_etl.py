import os
# import requests
# import zipfile
import pandas as pd
import numpy as np
import pydicom
import cv2
import psycopg2
from datetime import datetime
DATA_DIR = "C:/Users/sande/Downloads/dicom_dir"
IMAGE_DIR = "C:/Users/sande/OneDrive/Desktop/Processed images"
METADATA_FILE = os.path.join(DATA_DIR,"metadata.csv")
DB_NAME = "medical_db"
DB_USER = "postgres"
DB_PASSWORD = "Fall@2024abcd"
DB_HOST = "localhost"
DB_PORT = "5432"
os.makedirs(IMAGE_DIR,exist_ok=True)
def dicom_to_png(dicom_path,output_path):
    try:
        dcm = pydicom.dcmread(dicom_path)
        img = dcm.pixel_array
        img = (img - img.min()) / (img.max() - img.min()) * 255.0 
        img = img.astype(np.uint8)
        cv2.imwrite(output_path,img)
        return True
    except Exception as e:
        print(f"Error processing {dicom_path}:{e}")
        return False
def process_images(input_folder,output_folder):
    metadata = []
    for filename in os.listdir(input_folder):
        if filename.endswith(".dcm"):
            dicom_path = os.path.join(input_folder,filename)
            png_path = os.path.join(output_folder,f"{filename.replace('.dcm','.png')}")
            if dicom_to_png(dicom_path,png_path):
                dcm = pydicom.dcmread(dicom_path)
                study_date = datetime.strptime(dcm.StudyDate, "%Y%m%d").date()
                metadata.append({
                     "PatientID" : dcm.PatientID,
                     "Modality":dcm.Modality,
                     "StudyDate":study_date,
                     "ImagePath":png_path
                })
    df = pd.DataFrame(metadata)
    df.to_csv(METADATA_FILE,index=False)
    print(f"Processed {len(metadata)} images. Metadata Saved to METADATA_FILE")
def connect_db():
    return psycopg2.connect(database=DB_NAME,user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT)
def insert_data():
    conn = connect_db()
    cursor = conn.cursor()
    df = pd.read_csv(METADATA_FILE)
    for index,row in df.iterrows():
        cursor.execute("INSERT INTO medical_images (patient_id, modality, study_date, image_path) VALUES (%s, %s, %s, %s);",(row['PatientID'],row['Modality'],row['StudyDate'],row['ImagePath']))
    conn.commit()
    conn.close()
    print("Metadata inserted into database")
def query_data():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM medical_images LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()
def run_etl_pipeline():
    print("Running ETL Pipeline")
    process_images(DATA_DIR,IMAGE_DIR)
    insert_data()
    query_data()
    print("ETL Pipeline Completed")
run_etl_pipeline()