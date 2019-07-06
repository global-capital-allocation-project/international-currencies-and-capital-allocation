#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Master_Build_Controller
#
# This backend bash script manages the dispatch of all build jobs that run in Stata.
# Notes:
#   - The module calls (module load [...]) correspond to those used on the SLURM-based system on the
#   Odyssey research cluster at Harvard. These will likely have to be adapted when running on
#   different systems.
# - Please replace the following with the appropriate variables/paths for your system: <USERNAME>.
# --------------------------------------------------------------------------------------------------

# INPUT ENVIRONMENT VARIABLES
echo "USER="${1}
echo "STEP="${2}

# OUTPUT ENVIRONMENT VARIABLES
echo "SLURM_JOB_ID="$SLURM_JOB_ID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR
echo "SLURM_ARRAY_TASK_ID="$SLURM_ARRAY_TASK_ID
echo "SLURM_ARRAY_JOB_ID"=$SLURM_ARRAY_JOB_ID
echo "SLURM_ARRAY_TASK_ID"=$SLURM_ARRAY_TASK_ID
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR

# LOAD MODULES
case ${1} in 
  "<USERNAME>")
    module load stata/14.0-fasrc01
    ;;
esac

# RUN CALCULATIONS
umask 007
stata-mp -b "${mns_code_path}/build/Master_Build.do" ${1} ${2} $SLURM_ARRAY_TASK_ID ${3}
rm -f Master_Build.log

# FINISHED
echo "Finished Step "${2}
exit
