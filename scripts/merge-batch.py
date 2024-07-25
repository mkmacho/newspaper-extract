import argparse
import pandas as pd
import os
from common import add_filepath_suffix

def main():
    ''' Concatenate and join batched extractions. '''

    # Load data
    nrows = args.batch_size * args.nbatches if args.nbatches else None

    if args.filepath.endswith('.gzip'):
        sample = pd.read_parquet(args.filepath, columns=args.cols)
        if nrows: sample = sample.iloc[:nrows]
    else:
        sample = pd.read_csv(args.filepath, nrows=nrows, usecols=args.cols, index_col=[0])
    sample.raw_content = sample.raw_content.fillna('')

    if args.skip:
        sample = sample.iloc[args.skip:]
    print("Loaded template data of {} rows.".format(len(sample)))

    newspaper = args.filepath.split('/')[-1].split('-')[0]

    # Concatenate extraction batches
    full_extractions = pd.DataFrame()
    nbatches = args.nbatches or (len(sample) // args.batch_size + 1)
    print("Iterating over {} batches.".format(nbatches))
    for batch_idx in range(nbatches):
        batch = args.batch_size * (batch_idx + 1) + args.skip
        file = os.path.join(args.batch_dir, '-'.join([newspaper, args.suffix, 'batch', str(batch)]) + '.gzip')
        if not os.path.isfile(file): 
            print("File not found: '{}'".format(file))
            break
        full_extractions = pd.concat([full_extractions, pd.read_parquet(file)])
        print("After batch {}, have extractions of shape {}.".format(file, full_extractions.shape))

    assert len(full_extractions) == len(sample)
    sample = sample.join(full_extractions)

    # Write full data to file
    # sample.to_csv(add_filepath_suffix(args.output_dir, newspaper, n=len(sample), suffix='extract-wage', ext='csv'))        
    print("Have final shape of {}.".format(sample.shape))
    sample.to_parquet(add_filepath_suffix(args.output_dir, newspaper, n=len(sample), 
        suffix='{}-merged'.format(args.suffix)), compression='gzip')

    if args.delete:
        for batch_idx in range(nbatches):
            batch = args.batch_size * (batch_idx + 1) + args.skip
            file = os.path.join(args.batch_dir, '-'.join([newspaper, args.suffix, 'batch', str(batch)]) + '.gzip')
            if not os.path.isfile(file): 
                break
            os.remove(file)
            print("Removed batch {}.".format(file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', type=str, help="Filepath to template data.")
    parser.add_argument('--batch_dir', type=str, help="Filepath to directory of batches",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
            "3_Data_processing/4-output/7-geolocation/")
    parser.add_argument('-n', '--nbatches', type=int, default=None, help="Limit size.")
    parser.add_argument('-s', '--suffix', type=str, default='resolve', help="Batches of what.")
    parser.add_argument('-b', '--batch_size', type=int, default=10000, help="Batch size.")
    parser.add_argument('--cols', action='append', default=None, help="Columns to read from batch.")
    parser.add_argument('-d', '--delete', type=int, default=1, help="Delete batches.")
    parser.add_argument('--skip', type=int, default=0)
    parser.add_argument('-o', '--output_dir', type=str, help="Filepath to output directory.",
        default="/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/" \
            "3_Data_processing/4-output/")
    
    args = parser.parse_args()

    assert os.path.isfile(args.filepath), 'Invalid filepath to template data.'
    assert os.path.isdir(args.batch_dir), 'Invalid filepath to batch directory.'
    assert os.path.isdir(args.output_dir), 'Invalid filepath to output directory.'
    main()

