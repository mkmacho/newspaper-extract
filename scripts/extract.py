import time
import os 
import argparse
import pandas as pd
from math import ceil
from common import TextWrapper, USGeoData, add_filepath_suffix, time_now
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


class Newspaper(object):
    def __init__(self, newspaper, US_DATA, TEXT_HELP, min_pop=50000):
        assert newspaper in US_DATA.NEWSPAPER_TO_STATE_ID
        self.newspaper = newspaper
        self.US_DATA = US_DATA.load(newspaper)
        self.TEXT_HELP = TEXT_HELP
        print("Newspaper class loaded.")

    def city_state_options(self, tokens_list:list, idx:int):
        ''' Return possible city and state of address.
        If tokens following marker *seem like* potential city or state, include. 
        '''
        def get_possibles(arrays:list):
            possibles = []
            for arr in arrays:
                arr = ' '.join([word for word in arr if (len(word) > 1 and not word.isdigit())])
                if not arr: continue
                possibles.append(arr)
            return list(set(possibles))

        possible_cities = get_possibles([tokens_list[idx+1:idx+2],
                            tokens_list[idx+2:idx+3],tokens_list[idx+1:idx+3],
                            tokens_list[idx+2:idx+4]])
        possible_states = get_possibles([tokens_list[idx+1:idx+2],
                            tokens_list[idx+2:idx+3],tokens_list[idx+3:idx+4]])
        # Check for possible misspelled cities and states in post road marker tokens
        cities_dict_dict = self.US_DATA.check_nearby_cities(possible_cities)
        states_dict_dict = self.US_DATA.check_nearby_states(possible_states)
        return self.US_DATA.possible_city_state(self.US_DATA.state_name, 
            self.US_DATA.nearby_states, cities_dict_dict, states_dict_dict)

    def extract(self, ad_text):
        ''' 
        Extract possible address from tokens surrounding road markers.
        Main assumption is that for a road marker (e.g. "street") the idx token in token_list,
        token idx-2 will be the number, token idx-1 the street name, 
        token idx+1 the city, and token idx+2 the state. 

        Returns:
            address_dicts_list: list of structured addresses
        '''
        address_dicts_list = []
        if not ad_text: return address_dicts_list
        if not isinstance(ad_text, str):
            print('ERROR. Ad text supplied:', ad_text)
            return address_dicts_list

        tokens_list = self.TEXT_HELP.clean_tokenize(ad_text, self.newspaper)
        if not tokens_list: return address_dicts_list

        # Upon detecting street marker, form extracted geolocation
        for i, word in enumerate(tokens_list):
            if i == 0: continue
            if word.lower() in self.TEXT_HELP.STREET_MARKERS:
                prefix = self.TEXT_HELP.find_street(tokens_list, i)
                suffixes = self.city_state_options(tokens_list, i) 
                assert suffixes
                for suffix in suffixes:
                    address = prefix | suffix
                    if not address in address_dicts_list: 
                        address_dicts_list.append(address)

        # Complement that with zipcodes (which also lead directly to county)
        zipcode_objects = self.US_DATA.find_nearby_zipcodes(ad_text, self.US_DATA.nearby_state_ids)
        zipcodes, added_zipcodes = [z.zip for z in zipcode_objects], []
        
        # For detected cities, check if detected zipcodes found in said cities
        for city_object in self.US_DATA.check_nearby_cities(tokens_list).values():
            for i, row in self.US_DATA.city_objects(city_object['name']).iterrows():
                for matched_zipcode in set(zipcodes) & set(row['zips'].split()):
                    added_zipcodes.append(matched_zipcode)
                    address_dicts_list.append({
                        'city':city_object['name'],
                        'state':row['state_name'],
                        'county':row['county_name'],
                        'zipcode':matched_zipcode}
                    )
        address_dicts_list.extend([{'zipcode':z} for z in zipcodes if z not in added_zipcodes])
        return address_dicts_list

    def employer_info(self, ad_text, sandbox=False, extract_employer=False):
        ''' Mirror extract, find *EMPLOYER NAME* and *OFFERED WAGE*.
        In theory would've done both at same time.
        '''
        employer_dict = {'wage':None}
        if sandbox: employer_dict.update(
            {'_wage_pred_strong':[],'_wage_pred_maybe':[],'_wage_pred_weak':[]})
        if not ad_text or not isinstance(ad_text, str): return employer_dict

        # Keep only first ad and skip over non-labor ads
        text = ad_text.split("_classifiedad_")[0]
        if any(term in text for term in self.TEXT_HELP.REAL_ESTATE) and not any(
            word in text for word in self.TEXT_HELP.NOT_RE): 
            return employer_dict

        # Try to extract employer name
        if extract_employer:
            employer_dict['employer'] = self.TEXT_HELP.extract_pos_employer(text)

        text = self.TEXT_HELP.clean_for_wage(text)
        if not text: return employer_dict

        tokens_list = text.split()
        best_candidates, potential_candidates, weak_candidates = [], [], []
        for i, word in enumerate(tokens_list):
            # When find potential salary, format
            if self.TEXT_HELP.potential_salary(word):
                best, potential, weak = self.TEXT_HELP.format_wage_candidate(tokens_list, i)
                if best: best_candidates.append(best)
                if potential: potential_candidates.append(potential)
                if weak: weak_candidates.append(weak)

        # Output best choice
        def choose_best_salary(options:list):
            salary = None
            if not options: return salary
            if len(options) > 1:
                options.sort(key=len)
                # Only keep larger substrings
                options = [wage for i, wage in enumerate(options) if not any(wage in opt for opt in options[i+1:])]
                for wage in options:
                    # Ensure best option has RATE
                    if any(rate in wage for rate in (self.TEXT_HELP.RATES_SINGLE | self.TEXT_HELP.RATES_DOUBLE)):
                        salary = wage
                        break
            return salary or options[0]
    
        employer_dict['wage'] = choose_best_salary(best_candidates or potential_candidates)
        if sandbox:
            employer_dict['_wage_pred_strong'] = best_candidates
            employer_dict['_wage_pred_maybe'] = potential_candidates
            employer_dict['_wage_pred_weak'] = weak_candidates
        
        return employer_dict



def multiprocessing(func, args, max_workers:int=None):
    with ProcessPoolExecutor(max_workers) as ex:
        res = ex.map(func, args)
    return list(res)


def multithreading(func, args, max_workers:int=None):
    with ThreadPoolExecutor(max_workers) as ex:
        res = ex.map(func, args)
    return list(res)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', type=str, help="Filepath to newspaper ads, e.g. " \
        "/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
        "3_Data_processing/4-output/6-final-datasets/ASA.csv")
    parser.add_argument('--extract_address', type=int, default=1)
    parser.add_argument('--extract_wage', type=int, default=0)
    parser.add_argument('-n', '--nrows', type=int, default=None, help="Maximum number of ads.")
    parser.add_argument('-m', '--multiprocessing', type=int, default=0, 
        help="Use multiprocessing.")
    parser.add_argument('-s', '--skip', type=int, default=0, help="Ads to skip at beginning.")
    parser.add_argument('-w', '--nworkers', type=int, default=None, help="Number workers to use.")
    parser.add_argument('-b', '--batch_size', type=int, default=100000, help="Batch size.")
    parser.add_argument('-a', '--aux_dir', type=str, help="Filepath to auxiliary directory.",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
            "3_Data_processing/1-code/auxiliary_files")
    parser.add_argument('-o', '--output_dir', type=str, help="Filepath to output directory.",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
            "3_Data_processing/4-output/7-geolocation/")
    args = parser.parse_args()

    assert os.path.isdir(args.aux_dir), 'Invalid filepath to auxilliary files.'
    assert os.path.isfile(args.filepath), 'Invalid filepath to data CSV.'
    assert os.path.isdir(args.output_dir), 'Invalid filepath to output directory.'

    # Load data
    sample = pd.read_csv(args.filepath, nrows=args.nrows, index_col=[0])
    sample.raw_content = sample.raw_content.fillna('')
    print("Will process sample of {} observations.".format(len(sample)))

    # Load Newspaper class with helper classes
    paper = os.path.splitext(args.filepath)[0].split('/')[-1]
    NEWSPAPER = Newspaper(
        newspaper=paper,  
        US_DATA=USGeoData(          # Load US data helper class
            os.path.join(args.aux_dir, "states.csv"),
            os.path.join(args.aux_dir, "simplemaps/uscities.csv"),
            os.path.join(args.aux_dir, "neighbors-states.csv")
        ),   
        TEXT_HELP=TextWrapper(    # Load text helper class
            os.path.join(args.aux_dir, "dictionary_list.txt")
        )
    )

    # Predict
    print("Beginning extractions using {} processing ({} workers) at {}.".format(
        'multi' if args.multiprocessing else 'serial', args.nworkers or 1, time_now()))
    start_time = time.time()
    args.batch_size = min(args.batch_size, len(sample))
    if args.multiprocessing:
        if args.extract_address:
            print("Extracting addresses...")
            sample['addresses'] = multiprocessing(NEWSPAPER.extract, sample.raw_content.to_list())
        if args.extract_wage:
            print("Extracting wages...")
            wages = pd.DataFrame()
            for batch_idx in range(ceil(len(sample) / args.batch_size)):
                if args.skip >= (batch_idx+1)*args.batch_size: continue
                wages_batch = pd.DataFrame(multiprocessing(NEWSPAPER.employer_info, 
                    sample.raw_content.iloc[batch_idx*args.batch_size:(batch_idx+1)*args.batch_size].to_list(),
                    max_workers=args.nworkers),
                    index=sample.index[batch_idx*args.batch_size:(batch_idx+1)*args.batch_size])
                wages = pd.concat([wages, wages_batch])
                print("Processed ads {}-{} at {}...".format(
                    batch_idx*args.batch_size,(batch_idx+1)*args.batch_size, time_now()))
                wages_batch.to_parquet(add_filepath_suffix(args.output_dir, paper, 
                    n=(batch_idx+1)*args.batch_size, suffix='extract-batch'), compression='gzip')
            sample = sample.join(wages)
    else:
        if args.extract_address:
            print("Extracting addresses...")
            sample['addresses'] = sample.raw_content.apply(NEWSPAPER.extract)
        if args.extract_wage:
            print("Extracting wages...")
            wages = pd.DataFrame()
            for batch_idx in range(ceil(len(sample) / args.batch_size)):
                if args.skip >= (batch_idx+1)*args.batch_size: continue
                wages_batch = pd.DataFrame(sample.raw_content.iloc[
                    batch_idx*args.batch_size:(batch_idx+1)*args.batch_size].apply(
                        NEWSPAPER.employer_info).to_list(),
                    index=sample.index[batch_idx*args.batch_size:(batch_idx+1)*args.batch_size])
                wages = pd.concat([wages, wages_batch])
                print("Processed ads {}-{} at {}...".format(
                    batch_idx*args.batch_size,(batch_idx+1)*args.batch_size, time_now()))
                wages_batch.to_parquet(add_filepath_suffix(args.output_dir, paper, 
                    n=(batch_idx+1)*args.batch_size, suffix='extract-batch'), compression='gzip')
            sample = sample.join(wages)
    
    sample.to_parquet(add_filepath_suffix(args.output_dir, paper, n=args.nrows), 
        compression='gzip')
    sample.to_csv(add_filepath_suffix(args.output_dir, paper, n=args.nrows, ext='csv'))
    elapsed = time.time() - start_time
    print("Completed extractions at {} in {} minutes ({} seconds).".format(
        time_now(), round(elapsed / 60, 2), round(elapsed)))



