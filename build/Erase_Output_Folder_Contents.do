* --------------------------------------------------------------------------------------------------
* Erase_Output_Folder_Contents
*
* This file erases the contents of output folders (for complete data regeneration).
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Erase_Output_Contents", replace

!rm -rf $mns_data/output
mkdir $output

log close
