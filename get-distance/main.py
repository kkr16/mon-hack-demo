from google.cloud import storage
import csv
from geopy import distance

def get_distance(event, context):
    print(f"Processing event: {event}.")
    storage_client = storage.Client()
    bucket = storage_client.bucket(event['bucket'])
    blob = bucket.blob(event['name'])
    tmp_file = "/tmp/" + event['name']
    blob.download_to_filename(tmp_file)
    
    list = []
    file = open(tmp_file)
    reader = csv.reader(file, delimiter=',')
    next(reader)
    for column in reader:
        if (bool(column[7]) & bool(column[1])):
            from_coordinates = [column[1],column[2]]
            to_coordinates =  [column[7],column[8]]
            dist = distance.distance(from_coordinates,to_coordinates).km
            list.append(dist)

    avg = sum(list)/len(list)
    output_file_name = event['name'] + "_avg"
    output_file_path = "/tmp/" + output_file_name
    text_file = open( output_file_path, "w")
    text_file.write(str(avg))
    text_file.close()
    obj = storage.Blob(output_file_name, bucket)
    obj.upload_from_filename(output_file_path)

    print(avg)


    
