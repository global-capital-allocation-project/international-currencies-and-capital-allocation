#!/bin/sh
#
#SBATCH -p <SLURM_PARTITION>         # try both partitions
#SBATCH -n 1                         # 1 core
#SBATCH -N 1                         # 1 node
#SBATCH --mem 60000                  # 2 GB RAM
#SBATCH -t 1-00:00                   # test for 1 day
#SBATCH -o process_folder_%j.out     # STDOUT
#SBATCH -e process_folder_%j.err     # STDERR
#SBATCH --mail-type=FAIL,END         # mail notifications

#set -x

echo "*** archive2stata.sh"
read sysname <<<$(uname -a)

# modules needed
module load centos6/0.0.1-fasrc01
module load p7zip/9.38.1-fasrc01
module load sas/9.3-fasrc01

archive_filepath=$1                         # grab file pathname
archive_dir=$( dirname $1 )                 # grab the directory it's in
abs_path=`cd $archive_dir; pwd`             # do some gymnastics to ensure we have the full path
# and re-norm the 
archive_filename=${archive_filepath##*/}    # grab the filename
archive_filepath=$abs_path/$archive_filename  # and re-norm the variable
archive_base=${archive_filename%.*}         # grab the filename base


echo "srcdir $srcdir"
echo "archive_filepath $archive_filepath"


# ensure these directories are there
mkdir -p $archive_dir/xml
mkdir -p $archive_dir/sas/$archive_base

#exit 1
echo "archive_dir $archive_dir"
echo "abs_path $abs_path"
echo "archive_filename $archive_filename"
echo "archive_filepath $archive_filepath"
echo "archive_base $archive_base"
echo

# run unzip & concat
# this took 15 min for a big file
# XML file is generated that has same basename as archive
$srcdir/concat_xml_from_archive_file.sh $archive_filepath

# move to our XML directory
mv $archive_dir/$archive_base.xml $archive_dir/xml

if [ -d <DATA_PATH> ]; then
    REGAL_TEMP=<DATA_PATH>/temp/sas/$$
    mkdir -p $REGAL_TEMP
    lfs setstripe -c 8 $REGAL_TEMP
else 
    echo "WARNING: Could not find either preset TEMP directories."
    echo "Creating temp/sas/$$ at $PWD..."
    mkdir -p ./temp/sas/$$
fi


# send input dir, output dir, and input filenam as comma-separated
for i in 1 2 3 4 5; do
  sas \
  -SYSPARM "$archive_dir/xml,$archive_dir/sas/$archive_base,$archive_base.xml" \
  -work    "$REGAL_TEMP" \
  -print   "$archive_dir/sas/$archive_base/$archive_base.lst" \
  -log     "$archive_dir/sas/$archive_base/$archive_base.log" \
  $srcdir/extract_morningstar_xml_data.sas && break || sleep 15; 
done

# do any cleanup
# 
# zip up xml file
# remove temp directories
for i in 1 2 3 4 5; do
  tar -zcf $archive_dir/xml/$archive_base.xml.tgz \
    --remove-files \
    -C $archive_dir/xml \
    $archive_dir/xml/$archive_base.xml && break || sleep 15; 
done
rm -rf $REGAL_TEMP

# done!
