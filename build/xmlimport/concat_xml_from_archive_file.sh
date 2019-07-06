#!/bin/bash

# get file passed to script
# create appropriate temp folders
# expand archive into temp folder
# iterate thru xmls and concat into one file
# remove temp folder

debug=0                             # setup debug & output function
function echodebug {
    if [ $debug -eq 1 ]; then
        >&2 echo "$@"   
    fi
}

if [ $debug -eq 1 ]; then 
  set -x 
fi

archive_pathname=$1                         # grab pathname
archive_filename=${archive_pathname##*/}    # grab the filename
archive_base=${archive_filename%.*}         # grab the filename base


function echodebug { 
    if [ $debug -eq 1 ]; then
        >&2 echo "$@"
    fi
}

# declare the files/folders we're going to use
temp_output=$archive_base.temp
final_output=$archive_base.xml

startdir=$( dirname $archive_pathname )      # grab the directory it's in

if [ $debug -eq 1 ]; then
  pid=0000
else
  pid=$$
fi 

tmp=/tmp
xml_temp_dir=$tmp/$pid/xml_temp_dir
xml_nobom_files=$tmp/$pid/xml_fileset_nobom
total_xml_count=0


echo "*** concat_xml_from_archive_file.sh"
echo "archive_pathname $archive_pathname"
echo "archive_filename $archive_filename"
echo "archive_base $archive_base"
echo "temp_output $temp_output"
echo "final_output $final_output"
echo "startdir $startdir"
echo "xml_temp_dir $xml_temp_dir"
echo "xml_nobom_files $xml_nobom_files"
echo


# ensure that folders are there and are empty
if [ -e $xml_nobom_files ] && [ $debug -eq 0 ]
then
  rm -rf $xml_nobom_files
fi
mkdir -p $xml_nobom_files

if [ -e $xml_temp_dir ] && [ $debug -eq 0 ]
then
  rm -rf $xml_temp_dir
fi
mkdir -p $xml_temp_dir

# and ensure that our final file doesn't already exist
if [ -e $final_output ]
then
  rm $final_output
fi


# start our output file:
#   add header & <file> tags
echo '<?xml version="1.0" encoding="utf-8"?>' > $startdir/$temp_output
echo '<File>' >> $startdir/$temp_output

echodebug "Expanding $archive_pathname"

# copy each file to temp dir; expand, and process...
#if ps aux | grep some_proces[s] > /tmp/test.txt; then echo 1; else echo 0; fi
7za e -y -o$xml_temp_dir $archive_pathname > /dev/null

xml_count=0
echodebug "Processing XML files"
for xml_file in $( ls -1 $xml_temp_dir/*.xml  )
# for each XML file
do
    # strip off path, so we can direct appropriately
    xml_file=${xml_file##*/}

    # convert from UTF-8 to ascii (remove BOM)
    iconv --from-code UTF-8 --to-code US-ASCII -c $xml_temp_dir/$xml_file > $xml_nobom_files/$xml_file
    
    #   remove XML header and append to output file
    #     doesn't matter if we grab this from XMLs with BOM or noBOM
    perl  -pe 's/^.+?utf-8"\?>//' $xml_nobom_files/$xml_file >> $startdir/$temp_output
    echo >> $startdir/$temp_output

    # do some cleanup, so the # of files in the dir doesn't get too large
    if [ $debug -eq 0 ]; then
      rm $xml_nobom_files/$xml_file
    fi

    # and track our count
    (( xml_count += 1 )) 
    (( total_xml_count += 1 ))
    
done

echodebug "Processed $xml_count XML files"

# cleanup our temp files/directories

# remove any xml files in tempdir to don't reconcat those before next zip file
if [ $debug -eq 0 ]; then
    rm -rf $tmp/$pid
fi

# add final </file> tag
echo '</File>' >> $startdir/$temp_output

# and now rename 
mv $startdir/$temp_output $startdir/$final_output

echo "# of files concatenated: $total_xml_count"
