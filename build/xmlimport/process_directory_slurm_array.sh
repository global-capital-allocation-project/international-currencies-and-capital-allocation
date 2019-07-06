#!/bin/bash

#set -x
suffix=7z

# WARNING: note that verbose=1 should not be used with Master_Build.sh
verbose=0

# Set this flag to:
#   1 to count number of 7z in destination folder autonomously
#   2 to rely on hardcoded numbers (NOTE: this option must be set for use with Master_Build.sh)
#   3 to rely on counts from find_num_files.sh
compute_file_count_type=2

# make sure we know where this script is running from


# look for exported code path and then use for srcdir
if [ -z $mns_code_path ]; then
    export srcdir=<CODE_PATH>/build/xmlimport
else 
    export srcdir=$mns_code_path/build/xmlimport
fi

if [ ${verbose} = 1 ]; then
    echo "*** process_directory_slurm_array.sh ***"
    echo "srcdir is: $srcdir"
fi

# ensure we got a folder; else abort...
if [ ! -d $1 ]; then
    echo "$1"
    echo "Error! The parameter must be a directory!"
    exit 1
fi

abs_path=`cd $1; pwd`           # grab absolute path

# get the file count
if [ ${compute_file_count_type} = 1 ]; then

    file_count=`ls -1 $abs_path/*.$suffix | wc -l`

elif [ ${compute_file_count_type} = 3 ]; then

    if [ -f "$abs_path/.filecount" ]; then 
        file_count=`cat "$abs_path/.filecount"`
        rm "$abs_path/.filecount"
    else
        echo "Error! Cannot find file count. Please use find_num_files.sh first."
        exit 1
    fi 

elif [ ${compute_file_count_type} = 2 ]; then

    if [[ $abs_path = *"morningstar_ftp_master"* ]]; then
        file_count=3226
    else
        echo "Error! Cannot recognize given target path with option compute_file_count_type=2."
        exit 1
    fi

else

    echo "Error! Value of compute_file_count_type not recognized."
    exit 1

fi

# make log directory
mkdir -p $abs_path/slurm_logs

# partition spec
partition_name="<SLURM_PARTITION>"

# run sas
if [ -z "$2" ]; then

    JOB_ID=`sbatch --array=1-$file_count -p $partition_name \
        -o $abs_path/slurm_logs/process_folder_item_%A_%a.out \
        -e $abs_path/slurm_logs/process_folder_item_%A_%a.err \
        $srcdir/process_archive_under_slurm_array.slurm $abs_path | awk '{print $NF}'`

else

    JOB_ID=`sbatch --array=1-$file_count -p $partition_name \
        -o $abs_path/slurm_logs/process_folder_item_%A_%a.out \
        -e $abs_path/slurm_logs/process_folder_item_%A_%a.err \
         --depend=afterok:${2} \
        $srcdir/process_archive_under_slurm_array.slurm $abs_path | awk '{print $NF}'`

fi

echo "$JOB_ID"

if [ ${verbose} = 1 ]; then
    echo "Submitted $file_count files in $abs_path as SLURM job array"
fi
