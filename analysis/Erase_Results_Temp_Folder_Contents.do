* This file Erases Contents of Results Temp Folder (For Complete Regeneration of Analysis)
 
cap log close
log using "$logs/${whoami}_Erase_Temp_Contents", replace

!rm -rf $mns_data/results/temp
mkdir $mns_data/results/temp
mkdir $mns_data/results/temp/graphs
mkdir $mns_data/results/temp/tables
mkdir $mns_data/results/temp/erroroutput

log close
