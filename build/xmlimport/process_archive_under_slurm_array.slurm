#!/bin/bash
#
#SBATCH -n 1                     # 1 core
#SBATCH -N 1                     # 1 node
#SBATCH --mem 40000              # 2 GB RAM
#SBATCH -t 0-07:00               # test for 3 days
#SBATCH -o process_folder_item_%A_%a.out    # STDOUT
#SBATCH -e process_folder_item_%A_%a.err    # STDERR
#SBATCH --mail-type=FAIL,END        # mail notifications

#set -x 

# base
suffix=7z

# getting full path from SLURM call
archive_dir=$1

# move from 1-based to 0-based index
slot=$(( $SLURM_ARRAY_TASK_ID - 1 ))

# pause so as not to overwhelm metadata controller
#   amount randome # between 1 and max array count
echo "*** process_archive_under_slurm_array.slurm"
sleep $[ ( $RANDOM % 100 )  + 1 ]s

# Assuming 7z archives, so get list
echo "starting ls operation"
for i in 1 2 3 4 5; do
	archive_list=( $( timeout --signal=SIGINT 20m  ls -1 $archive_dir/*.$suffix ) ) && break || sleep 15; 
done
archive_pathname=${archive_list[$slot]}

echo "srcdir $srcdir"
echo "slot $slot"
echo "archive_pathname $archive_pathname"
echo

# call our code!
$srcdir/archive2stata.sh \
    $archive_pathname
