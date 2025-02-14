from concurrent.futures import ThreadPoolExecutor
import requests
from requests.exceptions import RequestException, ReadTimeout
from statistics import mode
from math import ceil
import argparse
import pandas as pd
import os
import time
import numpy as np
from ast import literal_eval
from common import USGeoData, add_filepath_suffix, time_now


def get_wrapper(url, timeout=10):
    output = {'url':url, 'elapsed':None, 'content':{}, 'message':""}
    try:
        resp = requests.get(url, timeout=timeout)
        output['content'] = resp.json()
    except ReadTimeout as err:
        output['message'] = str(err) or ""
        output['elapsed'] = timeout
        resp = err
    except RequestException as err:
        output['message'] = str(err) or ""
        resp = err
    finally:
        output.update({
            'status_code':getattr(resp, "status_code", 404), 
            'type':type(resp).__name__ or ""
        })
        try:
            output['elapsed'] = resp.elapsed.total_seconds()
        except:
            pass
    return output

def nominatum_request(query, biggest_nearby_cities, timeout=10):
    url = "https://nominatim.openstreetmap.org/search?addressdetails=1&q={}&format=jsonv2".format(query)
    response = get_wrapper(url, timeout=timeout)
    assert isinstance(response, dict)
    counties, zipcodes, address = [], [], None
    if response.get('status_code') == 200:
        assert isinstance(response['content'], list)
        for verified in response['content']:
            if not verified.get('address'): continue
            if not verified['address'].get('city') in biggest_nearby_cities: continue
            county = verified['address'].get('county')
            if county: counties.append(county.split(' County')[0])
            zipcode = verified['address'].get('postcode')
            if zipcode: zipcodes.append(zipcode)
            if verified.get('display_name') and not address:
                # Just use first address
                address = verified['display_name']
    return address, mode(counties or [None]), mode(zipcodes or [None]), response

def geoapify_request(query, biggest_nearby_cities, timeout=10):
    url = os.environ['GEOAPIFY_URL'] + "/v1/geocode/search?text={}&apiKey={}".format(
        query, os.environ['GEOAPIFY_API_KEY'])
    response = get_wrapper(url, timeout=timeout)
    assert isinstance(response, dict)
    best_county, best_zipcode, address, best_conf = None, None, None, 0
    if response.get('status_code') == 200:
        assert isinstance(response['content'], dict)
        for verified in response['content'].get('features', []):
            if not verified.get('properties'): continue
            confidence = verified['properties'].get('rank', {}).get('confidence', 0)
            if confidence <= 0: continue
            if not verified['properties'].get('city') in biggest_nearby_cities: continue
            county = verified['properties'].get('county')
            if county and confidence > best_conf: best_county = county.split(' County')[0]
            zipcode = verified['properties'].get('postcode')
            if zipcode and confidence > best_conf: best_zipcode = zipcode
            if verified['properties'].get('formatted') and confidence > best_conf:
                address = verified['properties']['formatted']
    return address, best_county, best_zipcode, response

def format_str_address(address_fields:dict):
    assert isinstance(address_fields, dict)
    addr_str = ''
    number = address_fields.get('housenumber')
    street = address_fields.get('street')
    if street:
        addr_str = number + ' ' + street if number else street
    for field in ['city', 'state', 'zipcode']:
        if not address_fields.get(field): continue
        new_field = address_fields[field]
        addr_str = addr_str + ', ' + new_field if addr_str else new_field
    addr_str += ', USA'
    return addr_str

def resolve(address_dicts_list:list, US_DATA:object, nominatum=False, geoapify=True, verbose=False):
    st_time = time.time()
    output = {}

    if nominatum:
        nom_counties, nom_zipcodes, nom_addresses, nom_logs, nom_time = [], [], [], [], 0
    
    if geoapify:
        geo_counties, geo_zipcodes, geo_addresses, geo_logs, geo_time = [], [], [], [], 0
    
    for addr in address_dicts_list:
        query = format_str_address(addr)

        if nominatum:
            nst = time.time()
            time.sleep(1) # Avoid requests block
            address, county, zipcode, log = nominatum_request(query,
                US_DATA.biggest_nearby_cities)
            nom_addresses.append(address)
            nom_logs.append(log)
            if county: nom_counties.append(county)
            if zipcode: nom_zipcodes.append(zipcode)
            nom_time += time.time() - nst

        if geoapify:
            gst = time.time()
            address, county, zipcode, log = geoapify_request(query,
                US_DATA.biggest_nearby_cities)
            assert log 
            geo_addresses.append(address)
            geo_logs.append(log)
            if county: geo_counties.append(county)
            if zipcode: geo_zipcodes.append(zipcode)
            geo_time += time.time() - gst
           
    if len(address_dicts_list) > 0 and verbose:
        if nominatum:
            print("Nominatum API: {} seconds per request.".format(
                round(nom_time / len(address_dicts_list), 1)))
        if geoapify:
            print("GeoApify API: {} seconds per request.".format(
                round(geo_time / len(address_dicts_list), 1)))

    if geoapify: 
        geo_zip_counties = US_DATA.counties_from_zips(geo_zipcodes)
        output['geo_addrs'] = geo_addresses
        output['geo_county'] = mode(geo_counties) if geo_counties else None
        output['geo_zip_county'] = mode(geo_zip_counties or [None])
        output['geo_requests'] = geo_logs
    if nominatum: 
        nom_zip_counties = US_DATA.counties_from_zips(nom_zipcodes)
        output['nom_addrs'] = nom_addresses
        output['nom_county'] = mode(nom_counties) if nom_counties else None
        output['nom_zip_county'] = mode(nom_zip_counties or [None])
        output['nom_requests'] = nom_logs
    if nominatum and geoapify:
        output['same_county'] = output['nom_county'] == output['geo_county']
        output['same_zip_county'] = output['nom_zip_county'] == output['geo_zip_county']
    
    return output


def multithreading(func, addrs, geo, max_workers:int=None):
    with ThreadPoolExecutor(max_workers) as ex:
        res = ex.map(lambda x: func(x, geo), addrs)
    return list(res)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', type=str, help="Filepath to newspaper ads, e.g. " \
        "/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
        "3_Data_processing/4-output/6-final-datasets/ASA-extract.gzip")
    parser.add_argument('-n', '--nrows', type=int, default=None, help="Maximum number of ads.")
    parser.add_argument('-s', '--skip', type=int, default=0, help="Ads to skip at beginning.")
    parser.add_argument('-m', '--multithreading', type=bool, default=False, help="Use multithreads.")
    parser.add_argument('-w', '--nworkers', type=int, default=None, help="Number workers to use.")
    parser.add_argument('-b', '--batch_size', type=int, default=10000, help="Batch size.")
    parser.add_argument('-u', '--geoapify_url', type=str, default="https://api.geoapify.com", 
        help="GeoApify URL endpoint to ping.")
    parser.add_argument('-a', '--aux_dir', type=str, help="Filepath to auxiliary files.",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
                "3_Data_processing/1-code/auxiliary_files")
    parser.add_argument('-o', '--output_dir', type=str, help="Filepath to output directory.",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
            "3_Data_processing/4-output/7-geolocation/")
    args = parser.parse_args()

    assert os.path.isdir(args.aux_dir), 'Invalid filepath to auxilliary files.'
    assert os.path.isfile(args.filepath), 'Invalid filepath to data CSV.'
    assert os.path.isdir(args.output_dir), 'Invalid filepath to output directory.'

    print("Beginning geolocation validation.")

    # Load list of lists of candidates address dicts
    assert os.path.isfile(args.filepath)
    newspaper = args.filepath.split('/')[-1].split('-')[0]

    sample = pd.read_parquet(args.filepath).iloc[:args.nrows]
    assert sample.addresses.isna().sum() == 0, 'Have NAs in addresses, exiting.'
    assert sample.addresses.dtype == 'object', 'Wrong addresses dtype, exiting.'
    assert isinstance(sample.addresses.iloc[0], np.ndarray), 'Wrong addresses dtype, exiting.'
    print("Will resolve sample of {} observations from {}.".format(len(sample), newspaper))

    # Load US geo-data
    US_DATA = USGeoData(          
        os.path.join(args.aux_dir, "states.csv"),
        os.path.join(args.aux_dir, "simplemaps/uscities.csv"),
        os.path.join(args.aux_dir, "neighbors-states.csv")
    ).load(newspaper)

    # GeoApify API key: move to environ!
    os.environ['GEOAPIFY_URL'] = args.geoapify_url # Note: pro URL would be 'https://bk01.geoapify.net'
    print("Will make requests to GeoApify URL: '{}'".format(os.environ['GEOAPIFY_URL']))

    # Predict
    print("Beginning resolutions using {} threading ({} workers) at {}.".format(
        'multi' if args.multithreading else 'mono', args.nworkers or 1, time_now()))
    st_time = time.time()
    counties = pd.DataFrame()

    for batch_idx in range(ceil(len(sample) / args.batch_size)):
        if args.skip >= (batch_idx+1)*args.batch_size: continue
        batch = sample.addresses.iloc[batch_idx*args.batch_size:(batch_idx+1)*args.batch_size]
        indices = sample.index[batch_idx*args.batch_size:(batch_idx+1)*args.batch_size]
        if args.multithreading:
            counties_batch = pd.DataFrame(multithreading(resolve, batch.to_list(), 
                US_DATA, max_workers=args.nworkers), index=indices)
        else:
            counties_batch = pd.DataFrame(batch.apply(resolve, args=(US_DATA,)).to_list(),
                index=indices)
        try:
            counties_batch.to_parquet(add_filepath_suffix(args.output_dir, newspaper, 
                n=(batch_idx+1)*args.batch_size, suffix='resolve-batch'), compression='gzip')
        except Exception as e:
            print(f"Batch save failed: {str(e)}")
            print(counties_batch.geo_requests.iloc[:5].to_list())
        counties = pd.concat([counties, counties_batch])
        print("Processed ads {}-{} at {}...".format(
            batch_idx*args.batch_size,(batch_idx+1)*args.batch_size, time_now()))
        
    # sample = pd.merge(sample, counties, how='left')
    # sample = sample.join(pd.DataFrame(counties, index=sample.index))
    sample = sample.join(counties)
    sample.to_parquet(add_filepath_suffix(args.output_dir, newspaper, n=args.nrows or len(sample), 
        suffix='resolve'), compression='gzip')
    sample.to_csv(add_filepath_suffix(args.output_dir, newspaper, n=args.nrows or len(sample), 
        suffix='resolve', ext='csv'))    
    elapsed = time.time() - st_time
    print("Completed resolutions at {} in {} minutes ({} seconds).\n".format(
        time_now(), round(elapsed/60, 2), round(elapsed)))


