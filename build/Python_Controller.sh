#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Python_Controller
#
# This backend bash script manages the dispatch of all build jobs that run in Python.
# Notes:
# 	- The module calls (module load [...]) correspond to those used on the SLURM-based system on the
#		Odyssey research cluster at Harvard. These will likely have to be adapted when running on
#		different systems.
#	- Please replace the following with the appropriate variables/paths for your system:
#		<USERNAME>, <PYTHON2_ENV_PATH>, <PYTHON3_ENV_PATH>. The latter two are the paths to the
#		Python 2 and Python 3 conda environments on your system (which can be generated using the
#		file utils/gen_python_environments.)
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
echo "SLURM_NTASKS"=$SLURM_NTASKS

# LOAD MODULES
case ${1} in
  "<USERNAME>")
	module load p7zip/9.38.1-fasrc01
	module load Anaconda/5.0.1-fasrc01
	case ${2} in
		"Fuzzy_Merge_Train_Linker"|"Fuzzy_Merge_Find_Matches"|"Fuzzy_Merge_Finalize")
		source activate <PYTHON2_ENV_PATH>
		echo "Loaded Python 2 environment"
		;;
		"Unwind_MF_Positions_Step1"|"Unwind_MF_Positions_Step2"|"Ultimate_Parent_Aggregation")
		source activate <PYTHON2_ENV_PATH>
		echo "Loaded Python 3 environment"
		;;
	esac
    ;;
esac

# SUBPATHS
OUTPUT_PATH="${mns_data_path}/output"
SCRATCH_PATH="${mns_data_path}/temp"

# RUN CALCULATIONS
case ${2} in
  "Fuzzy_Merge_Train_Linker")
	python "${mns_code_path}/build/fuzzy/Fuzzy_Merge_Train_Linker.py" -n ${NUM_SHARDS} -a ${ASSET_CLASS} -c ${SLURM_NTASKS} -d ${mns_data_path}
  ;;
  "Fuzzy_Merge_Find_Matches")
    python "${mns_code_path}/build/fuzzy/Fuzzy_Merge_Find_Matches.py" -p ${SLURM_ARRAY_TASK_ID} -a ${ASSET_CLASS} -g ${GEOGRAPHY} -c 1 -sd ${SCRATCH_PATH} -o ${OUTPUT_PATH} -f ${DO_FULL_PASS} -r ${SKIP_MATCHED} -b 0 -v 0
  ;;
  "Fuzzy_Merge_Finalize")
	python "${mns_code_path}/build/fuzzy/Fuzzy_Merge_Finalize.py" -a ${ASSET_CLASS} -sd ${SCRATCH_PATH} -o ${OUTPUT_PATH}
  ;;
  "Unwind_MF_Positions_Step1")
	python "${mns_code_path}/build/unwind/Unwind_MF_Positions_Step1.py" -t ${SLURM_ARRAY_TASK_ID} -f 1986 -d ${mns_data_path}
  ;;
  "Ultimate_Parent_Aggregation")
	python "${mns_code_path}/build/up_aggregation/UP_Aggregation.py" -d ${mns_data_path}
  ;;
esac

# FINISHED
echo "Finished Step "${2}
exit
