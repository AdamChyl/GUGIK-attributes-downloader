import requests
import json
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import threading

# example input file content:
# 18,7411003;52,8615303
# 18,7416191;52,8616714
# 18,7425728;52,8619041
# 18,7426548;52,8615303

input_file = "kujawsko_pomorskie_coords.csv"

output_file = "plots.geojson" # GeoJSON won't work, remove the comma after the last feature in the file, I recommend using sublimne text editor


def get_wms_info(params, lng, lat, error_file):
    base_url = "https://integracja.gugik.gov.pl/cgi-bin/KrajowaIntegracjaEwidencjiGruntow"

    try:
        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code == 200:
            return response.content
        else:
            print(f"Error {response.status_code} for point - {lng}; {lat}: {response.text}")
            write_error_coords((lng, lat), error_file)

    except requests.RequestException as e:
        print(f"Error making WMS request for point - {lng}, {lat}: {e}")
        write_error_coords((lng, lat), error_file)

    except Exception as e:
        print(f"An unexpected error occurred for point - {lng}, {lat}: {e}")
        write_error_coords((lng, lat), error_file)

    return None

def write_error_coords(coords, error_file):
    lng, lat = coords
    with open(error_file, 'a', encoding='utf-8') as error_coords_file:
        error_coords_file.write(f'{lng:.8f};{lat:.8f}\n')

def process_point(point, executor, output_file, file_lock, error_file, pbar):
    lng, lat = point

    params = {
        'VERSION': '1.1.1',
        'SERVICE': 'WMS',
        'REQUEST': 'GetFeatureInfo',
        'LAYERS': 'dzialki,numery_dzialek,budynki',
        'QUERY_LAYERS': 'dzialki,numery_dzialek,budynki',
        'SRS': 'EPSG:4326',
        'WIDTH': '1570',
        'HEIGHT': '916',
        'TRANSPARENT': 'TRUE',
        'FORMAT': 'image/png',
        'BBOX': f'{lng},{lat},{lng},{lat}',
        'X': '458',
        'Y': '785'
    }

    future = executor.submit(get_wms_info, params, lng, lat, error_file)
    future.add_done_callback(lambda f: process_wms_response(f.result(), lng, lat, output_file, file_lock, pbar))

def process_wms_response(wms_response, lng, lat, output_file, file_lock, pbar):
    if wms_response is not None:
        try:
            root = ET.fromstring(wms_response.decode('utf-8'))

            properties = {}

            for layer in root.findall(".//{http://www.intergraph.com/geomedia/gml}Layer"):
                for attribute in layer.findall(".//{http://www.intergraph.com/geomedia/gml}Attribute"):
                    name = attribute.get("Name")
                    value = attribute.text

                    if name not in ["Informacje o pochodzeniu danych", "Informacje dodatkowe o działce", "Kod QR"]:
                        properties[name] = value

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                },
                "properties": properties
            }

            with file_lock:
                json.dump(feature, output_file, indent=2, ensure_ascii=False)
                output_file.write(',\n')

            pbar.update(1)

        except Exception as e:
            print(f"Error processing XML for point - {lng}, {lat}: {e}")

if __name__ == "__main__":
    coords_error_file = 'coords_error.csv'
    with open(input_file, 'r', encoding='utf-8-sig') as file, \
         open(output_file, 'w', encoding='utf-8') as output_file:

        output_file.write('{"type": "FeatureCollection", "features": [\n')

        lines = file.readlines()
        file_lock = threading.Lock()

        with tqdm(total=len(lines)) as pbar:
            with ThreadPoolExecutor(max_workers=20) as executor: # set max_workers to the number of threads you want to run
                for line in lines:
                    line = line.lstrip('\ufeff')
                    lng, lat = map(float, line.replace(',', '.').strip().split(';'))
                    # lng, lat = map(float, line.strip().split(';')) # use this line if your input file is in the format: 18.7411003;52.8615303

                    process_point((lng, lat), executor, output_file, file_lock, coords_error_file, pbar)

        output_file.write('\n]}\n')

    print("GeoJSON saved to the output.geojson file.")
