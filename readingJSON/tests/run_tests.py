import os
import time
import json
import random
import configparser
from ingestJSON import ingestJSON

config = configparser.RawConfigParser()
config.read(r'ingestJSON/config.properties')


def test_sum():
    assert sum([1, 2, 3]) == 6, "Should be 6"


def test_exec_time(filepath):

    start_time = time.time()
    print("Load testing started---------")
    ingestJSON.populate_health_collection(filepath)
    print("Load testing ended-----------")
    return time.time() - start_time


def make_json_files(no_entries, no_files):
    file_path = config.get('JSONSection', 'json.test.path')
    pathname = "testJSONs"+str(no_entries)+"_"+str(no_files)
    path = os.path.join(file_path, pathname)
    os.mkdir(path)

    for i in range(no_files):
        filename = "testJSONs"+str(no_entries)+"_"+str(i)+".json"
        entries = []
        print(filename)

        for j in range(no_entries):
            gender = random.choice(["Male", "Female"])
            height = random.randint(100, 200)
            weight = random.randint(30, 120)

            entry = {"Gender": gender, "HeightCm": height, "WeightKg": weight}
            entries.append(entry)

        with open(path+"/"+filename, 'w+') as p:
            json.dump(entries, p)

    return file_path + "/" + pathname


def make_jsons():

    # filepaths = [make_json_files(1000, 2), make_json_files(10000, 2)]
    filepaths = [make_json_files(100000, 1)]
    print("Files generated are", filepaths)

    for filepath in filepaths:
        print("Load testing took {} seconds".format(test_exec_time(filepath)))

    """
    Running for 2000 entries through 2 files
    Files are ['testJSONs//testJSONs1000_2']
    Will ingest file testJSONs//testJSONs1000_2/testJSONs1000_0.json
    After ingesting testJSONs//testJSONs1000_2/testJSONs1000_0.json, number of records in collection are: 3208
    Will ingest file testJSONs//testJSONs1000_2/testJSONs1000_1.json
    After ingesting testJSONs//testJSONs1000_2/testJSONs1000_1.json, number of records in collection are: 4208
    0.5910184383392334

    
    Running for 20000 entries through 2 files
    
    Files are ['testJSONs//testJSONs10000_2']
    Will ingest file testJSONs//testJSONs10000_2/testJSONs10000_0.json
    After ingesting testJSONs//testJSONs10000_2/testJSONs10000_0.json, number of records in collection are: 14208
    Will ingest file testJSONs//testJSONs10000_2/testJSONs10000_1.json
    After ingesting testJSONs//testJSONs10000_2/testJSONs10000_1.json, number of records in collection are: 24208
    5.691565275192261

    
    Running for 100000 entries through 1 file
    
    COMMAND LINE: python -m tests.run_tests.py
    testJSONs100000_0.json
    Files generated are ['testJSONs//testJSONs100000_1']
    Load testing started---------
    Will ingest file testJSONs//testJSONs100000_1/testJSONs100000_0.json
    After ingesting testJSONs//testJSONs100000_1/testJSONs100000_0.json, number of records in collection are: 100000
    Load testing ended-----------
    Load testing took 34.65544509887695 seconds

    """


make_jsons()
