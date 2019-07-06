* ---------------------------------------------------------------------------------------------------------------------------------------------------------
* Build files for Maggiori, Neiman, and Schreger: "International Currencies and Capital Allocation", forthcoming (2019) in Journal of Political Economy
* ---------------------------------------------------------------------------------------------------------------------------------------------------------

I. INTRODUCTION

The files contained in the folder "build" perform a complete build of the data used in the paper. This includes both the mutual fund and
ETF holdings data by Morningstar, and all other data sources used for the paper's analyses. The
output of this build is a series of clean data files that are of manageable sizes and are then
used by the analysis code to produce all the tables and figures in the paper.

* --------------------------------------------------------------------------------------------------

II. EXECUTING THE BUILD

As described in the overall README.txt, the bash script Master_Build.sh is the main executable file for the build. Launching Master_Build.sh 
will run the full build, start-to-finish. Each of the steps of the build are outlined in the file, 
with short descriptions of their functions.

This file should be called as: ./Master_Build.sh <USERNAME>

* --------------------------------------------------------------------------------------------------

III. TECHNICAL NOTES

  - Please note that this build is built to be executed on a SLURM cluster computing environment,
    such as the Odyssey research cluster at Harvard and the Midway research cluster at the University
    of Chicago. The build may need to be adjusted in order to work in different environments. SLURM
    module calls are modeled after those that would be needed on the Odyssey cluster at Harvard and
    may need to be adapted if the code is run on different systems.

  - Prior to running the build, please be sure to perform a find-and-replace in the build folder for
    the following expressions. These are user- and system-specific build parameters that will need
    to be filled in accordingly. Individual build files also point out these parameters whenever
    they are present:

           <USERNAME>: Your username on the host system
           <CODE_PATH>: Path to the build code on the host system
           <DATA_PATH>: Path to the folder containing the data, in which the build is executed
           <USER_EMAIL>: Email of the user (for SLURM notifications)
           <SLURM_PARTITION>: The name of the SLURM partition on which the jobs are to be executed
           <PYTHON2_ENV_PATH>: Path to Python 2 Conda environment on your system; this environment
           					   can be generated using utils/gen_python_environment.sh
           <PYTHON3_ENV_PATH>: Path to Python 3 Conda environment on your system; this environment
           					   can be generated using utils/gen_python_environment.sh
           <API_KEY>: API key for the OpenFIGI service.

  -	In order to set the Stata environment, the build code assumes that the following global variables
	have been set in your profile.do file, so that these are automatically defined in Stata:
		
		$whoami: Your username
		$mns_code: Path to the build code on the host system
		$mns_data: Path to the folder containing the data, in which the build is executed

* --------------------------------------------------------------------------------------------------

IV. INPUT DATA

Please see the accompanying file MNS_Data_Guide.pdf for a list of all the raw input files that are
required in order to run the MNS build and analysis. All paths in the guide are relative to a master 
data folder referred to a "$mns_data". Users should use the folder structure outlined in the guide
in order for the build code to run as-is.

Files marked with [*] in the guide are publicly available and are included in the accompanying 
replication packet. Files not market with [*] must be purchased from the respective data providers; 
in these cases the guide provides information as to how the data can be sourced:
