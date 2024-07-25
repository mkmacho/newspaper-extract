#!/bin/bash

#SBATCH --mail-user=camacho.horvitz@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --partition=high		## high/low/gpu, default if empty is low
#SBATCH --job-name=resolve      ## Name of the job
#SBATCH --output=resolve.log    ## Output file
#SBATCH --ntasks=1             	## Number of tasks (analyses) to run
#SBATCH --cpus-per-task=20      ## The number of threads the code will use
#SBATCH --mem-per-cpu=250M     	## Real memory(MB) per CPU required by the job.
#SBATCH --time=7-00:00:00		## Week limit
	
## Load the python interpreter
module load python

srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/LAT-extract-all.gzip --nworkers=20 --multithreading=1 
srun echo "Finished LAT (8.2M ads)"

### TO RUN: sbatch -C mem768g job.sh
