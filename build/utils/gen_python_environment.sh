#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Gen_Python_Environment
#
# This script reproduces the Python conda environments used for the project. Please be sure to fill 
# in the parameters <PYTHON2_ENV_PATH> and <PYTHON3_ENV_PATH> with your preferred paths.
#
# Notes:
# 	- The module calls (module load [...]) correspond to those used on the SLURM-based system on the
#		Odyssey research cluster at Harvard. These will likely have to be adapted when running on
#		different systems.
# --------------------------------------------------------------------------------------------------
module load gcc/4.8.2-fasrc01 gtk/2.24.31-fasrc01
module load Anaconda/5.0.1-fasrc01
conda create -p <PYTHON2_ENV_PATH> python=2.7 anaconda
source activate <PYTHON2_ENV_PATH>
pip install tqdm
pip install tenacity
pip install dedupe

conda create -p <PYTHON3_ENV_PATH> python=3.6 anaconda
source activate <PYTHON3_ENV_PATH>
pip install tqdm
pip install tenacity
pip install dedupe
