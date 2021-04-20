from ingestJSON import ingestJSON
import configparser

config = configparser.RawConfigParser()
config.read(r'ingestJSON/config.properties')

if __name__ == '__main__':
    file_path = config.get('JSONSection', 'json.path')
    ingestJSON.populate_health_collection(file_path)
