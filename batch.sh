#!/bin/bash

#SBATCH --mail-user=camacho.horvitz@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --job-name=resolve      ## Name of the job
#SBATCH --output=resolve.log    ## Output file
#SBATCH --ntasks=1             	## Number of tasks (analyses) to run
#SBATCH --cpus-per-task=20      ## The number of threads the code will use
#SBATCH --mem-per-cpu=250M     	## Real memory(MB) per CPU required by the job.
#SBATCH --time=2-00:00:00		## No limit
	
## Load the python interpreter
module load python

## Execute the python script 
srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/BaS-extract-all.gzip --nworkers=20 --multithreading=1 -s=320000
srun echo "Finished BaS 3.7M"

## srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/ChT-extract-all.gzip --nworkers=20 --multithreading=1 
## srun echo "Finished ChT 6.5M, exp 4days"

## srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/HaC-extract-all.gzip --nworkers=20 --multithreading=1 
## srun echo "Finished HaC 2.1M"

## srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/LAT-extract-all.gzip --nworkers=20 --multithreading=1 
## srun echo "Finished LAT 8.2M"

## srun python /accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/1-code/miguel-test/resolve.py --filepath=/accounts/projects/pkline/newslabor/Documents/Newspaper_2023/3_Data_processing/4-output/7-geolocation/WaP-extract-all.gzip --nworkers=20 --multithreading=1 
## srun echo "Finished WaP 3M"
