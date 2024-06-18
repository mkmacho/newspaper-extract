# newspaper-extract

## Newslabor data synthesis for *The Origins of Racial Discrimination in US Labor Markets*

## Authors: Ellora Derononcourt, Joan Martinez, and Miguel Camacho Horvitz

Here we publish the extraction procedure for fields of interest from roughly 34 million vacancy (i.e. job advertisement listings) postings data. The results can be found directly on Berkeley's EML computer cluster at `~/Documents/Newspaper_2023/3_Data_processing/4-output/`, here we describe how to replicate those data.

The initial data look like ![data](example_images/data.png). Contact Joan Martinez for original data access from ProQuest DFM. 

We extract features from thirteen newspapers using the following abbrevations: ASA, ATC, ATL, BaS, BoG, ChT, HaC, LAS, LAT, NJG, NYr, NYT, and WaP.

## Installation

To run locally, we advise you create a virtual environment so as to install necessary packages in a clean environment, guaranteed of no clashing dependencies.

```bash
	python3 -m venv venv
	source ./venv/bin/activate
```

Install packages with `pip`

```bash
	pip install -r requirements.txt
```


## App Structure ##

```
└── newspaper-extract/
    ├── README.md
    ├── batch.sh
    ├── scripts/
    ├──── common.py
    ├──── extract.py
    ├──── resolve.py
    ├──── merge-batch.py
    ├── test_data/
    ├──── NJG.csv
    ├── example_images/
    ├──── data.png
    ├──── ...
    ├── auxiliary_files/
    ├──── states.csv
    ├──── ...
```

###### extract.py ######

Extraction of features happens here.

###### resolve.py ######

Validation of addresses happens here.

###### common.py ######

Any commonly used functions/classes will sit here.


## Sample usage

### extract.py ###

Given a path to a newspaper's advertisements (e.g. `/test_data/NJG.csv` from the historical Norfolk Journal & Guide) corresponding to ad-level observations containing `raw_content` feature of ad text, we can run `scripts/extract.py` to *extract* geolocation candidates of text as follows.

`python scripts/extract.py --filepath=<PATH_TO_AD_CSV_FILE>  --aux_dir=<PATH_TO_AUXILIARY_DATA_FILES> --output_dir=<PATH_TO_OUTPUT_DIRECTORY>` 

Example run using `NJG.csv` located in `./test_data/`:
```
python scripts/extract.py --filepath=./test_data/NJG.csv  --aux_dir=./auxiliary_files --output_dir=./test_data
```

Example ouput written to e.g. `./test_data/NJG-extract-all.gzip`, containing all columns from `NJG.csv` plus an additional column `addresses` of objects that look like, e.g.
```
array([
	{
		'housenumber': '509',
		'street': 'Main Street',
		'city': None,
		'county': None, 
		'state': 'Virginia',  
		'zipcode': None
	},
    {
    	'housenumber': None,
    	'street': None,
    	'city': 'Norfolk', 
    	'county': 'Norfolk',  
    	'state': 'Virginia', 
    	'zipcode': '23501'
    }
], dtype=object)
```
as in ![pred-geo](example_images/extract_geolocation.png).

Note that we could alternatively turn on the `extract_wage` flag:
`python scripts/extract.py --extract_wage=1 --filepath=<PATH_TO_AD_CSV_FILE>  --aux_dir=<PATH_TO_AUXILIARY_DATA_FILES> --output_dir=<PATH_TO_OUTPUT_DIRECTORY>`
in which case we would *additionally* extract a candidate wage (i.e. salary) from each job ad. In this case, `./outputs/NJG-extract-all.gzip`, will contain an additional `wage` feature of strings which look like, e.g. `$60 per hour` as in ![pred-wage](example_images/extract_wage.png).

Then, given the *candidate* `addresses` we identified, we can *validate* and identify the *county* field from the validated addresses using a (business) geocoding API. In this code, we use [GeoApify](https://www.geoapify.com/geocoding-api)'s API as follows.

### resolve.py ###

Given a filepath to a dataset containing candidate `addresses`, we can validate said addresses calling
`python scripts/resolve.py --filepath=<FILEPATH_TO_ADDRESS_DATA_FILE> --aux_dir=<PATH_TO_AUXILIARY_DATA_FILES> --output_dir=<PATH_TO_OUTPUT_DIRECTORY>`

Or, again using the `NJG` example data:
```
python scripts/resolve.py --filepath=./test_data/NJG-extract-all.gzip  --aux_dir=./auxiliary_files --output_dir=./test_data
```

which will output the input dataset plus additional columns `geo_addrs`, `geo_county`, `geo_zip_county`, and (potentially) `geo_requests` as in ![pred-resolve](example_images/resolve_geolocation.png).


### Additional Notes ###

Note that in EML we can leverage additional resources and run concurrent code, especially when waiting on API requests. Then it is helpful to consider running multithreading.

To make *final* dataset, i.e. those found in EML `/9-final/`, for a given newspaper we can run:

```
import pandas as pd

def merge_final(newspaper:str):
	df = pd.read_parquet("./7-geolocation/{newspaper}-resolve-all.gzip".format(newspaper=newspaper))
	wages = pd.read_parquet("./8-employer/{newspaper}-extract-all.gzip".format(newspaper=newspaper))
	assert len(df) == len(wages)
	df['wage'] = wages['wage']
	df.to_parquet("./9-final/{newspaper}.gzip".format(newspaper=newspaper), compression='gzip')
	df.to_csv("./9-final/{newspaper}.csv".format(newspaper=newspaper))

```

And to recover geo-location statistics about our data:

```
import pandas as pd
import os

for file in os.listdir():
	if not file.endswith("resolve-all.gzip"): continue
	print("On {}".format(file))
	df = pd.read_parquet("./{}".format(file))
	df['counties'] = pd.Series(np.where(df.geo_county.isna(), df.geo_zip_county, df.geo_county))	
	# print(df.shape)
	print("Addresses:", sum(df.addresses.str.len() > 0))
	print("Geo addrs:", sum(df.geo_addrs.notna()))
	print("Geo county:", sum(df.geo_county.notna()))
	print("Zip county:", sum(df.geo_zip_county.notna()))
	print("Any county:", sum(df.counties.notna()))

```


## TODO

- **List sources for auxiliary files.**
- **Finish README.**

## Issues

- **Loading large CSV files:** When load data, e.g. `df = pd.read_csv(filepath)`, get warning message: 
```
extract.py:114:DtypeWarning:Columns (19,20,21,22) have mixed types. Specify type option on import or set low_memory=False.
```
	- Should look into those columns. Do we see speed-up if specify dtypes?
- **Multi-processing:** When trying to `extract` using multi-processing in EML cluster get memory allocation error: ![memory](example_images/memory.png)
    - Should talk to Rowilma if becomes necessary.



3