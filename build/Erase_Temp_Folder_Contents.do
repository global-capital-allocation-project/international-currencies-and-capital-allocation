* --------------------------------------------------------------------------------------------------
* Erase_Temp_Folder_Contents
*
* This file erases the contents of temp folders (for complete data regeneration).
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Erase_Temp_Contents", replace

!rm -rf $mns_data/temp
mkdir $temp
mkdir $temp/erroroutput

log close
