#!/bin/bash
# --------------------------------------------------------------------------------------------------
# Master_Build
#
# This bash script is the main executable file for the build. Launching Master_Build.sh will run the
# full build, start-to-finish. Each of the steps of the build are outlined below, with short
# descriptions of their functions.
#
# This file should be called as: ./Master_Build.sh <USERNAME>
# 
# Technical notes:
#   - Please note that this script is built to be executed on a SLURM cluster computing environment,
#     such as the Odyssey research cluster at Harvard. The build may need to be adjusted in order
#     to work in different environments.
#   - Prior to running the build, please be sure to fill in the following parameters in the script
#     below:
#           <USERNAME>: Your username on the host system
#           <CODE_PATH>: Path to the build code on the host system
#           <DATA_PATH>: Path to the folder containing the data, in which the build is executed
#           <USER_EMAIL>: Email of the user (for SLURM notifications)
#           <SLURM_PARTITION>: The name of the SLURM partition on which the jobs are to be executed
# --------------------------------------------------------------------------------------------------
echo "Begin Master Build for MNS Project ..."

# ERROR CHECKS
if [[ $# -ne 1 ]]; then
  echo "Illegal number of parameters: input 1 must be user first name (lowercase)"
  exit
fi

# DEFINE USER SCRIPT
U=${1}
echo "User: "${U}

# SET THIS FLAG TO 1 TO RUN THE XML IMPORT STEPS
run_xml_import=0

# INPUT PARAMETERS THAT DO NOT VARY ACROSS USERS
nodes=1
mailtype="FAIL"

# STORE BUILD DIRECTORY PATH
BUILD_DIR="$(cd "$(dirname "$0")" && pwd)"

# INPUT PARAMETERS THAT DO VARY ACROSS USERS
case ${U} in

    "<USERNAME>")

        case ${U} in
          "<USERNAME>")
            mns_code_path="<CODE_PATH>"
            mns_data_path="<DATA_PATH>"
            mailuser="<USER_EMAIL>"
            ;;
        esac
        mns_erroroutput_path="${mns_data_path}/temp/erroroutput"
        mkdir -p "${mns_erroroutput_path}"

        partition_Get_Started="<SLURM_PARTITION>"
        time_Get_Started="0-00:30:00"
        ntasks_Get_Started=4
        mem_Get_Started="62000"

        partition_XML_Import_Prepare="<SLURM_PARTITION>"
        time_XML_Import_Prepare="0-8:00:00"
        ntasks_XML_Import_Prepare=4
        mem_XML_Import_Prepare="128000"

        partition_XML_Import_Step1="<SLURM_PARTITION>"
        time_XML_Import_Step1="0-18:00:00"
        ntasks_XML_Import_Step1=16
        mem_XML_Import_Step1="128000"

        partition_XML_Import_Step3="<SLURM_PARTITION>"
        time_XML_Import_Step3="0-10:00:00"
        ntasks_XML_Import_Step3=4
        mem_XML_Import_Step3="128000"

        partition_Public_Portfolios_Build="<SLURM_PARTITION>"
        time_Public_Portfolios_Build="0-02:00:00"
        ntasks_Public_Portfolios_Build=4
        mem_Public_Portfolios_Build="62000"

        partition_Macro_Build_Step1="<SLURM_PARTITION>"
        time_Macro_Build_Step1="0-01:00:00"
        ntasks_Macro_Build_Step1=4
        mem_Macro_Build_Step1="62000"

        partition_Macro_Build_Step2="<SLURM_PARTITION>"
        time_Macro_Build_Step2="0-01:00:00"
        ntasks_Macro_Build_Step2=4
        mem_Macro_Build_Step2="200000"

        partition_Orbis_Build_Step1="<SLURM_PARTITION>"
        time_Orbis_Build_Step1="0-01:00:00"
        ntasks_Orbis_Build_Step1=4
        mem_Orbis_Build_Step1="100000"

        partition_Orbis_Build_Step2="<SLURM_PARTITION>"
        time_Orbis_Build_Step2="0-12:00:00"
        ntasks_Orbis_Build_Step2=4
        mem_Orbis_Build_Step2="130000"

        partition_Orbis_Build_Step3="<SLURM_PARTITION>"
        time_Orbis_Build_Step3="0-08:00:00"
        ntasks_Orbis_Build_Step3=4
        mem_Orbis_Build_Step3="250000"

        partition_Dealogic_Build="<SLURM_PARTITION>"
        time_Dealogic_Build="0-05:00:00"
        ntasks_Dealogic_Build=4
        mem_Dealogic_Build="150000"

        partition_Drop_Fields="<SLURM_PARTITION>"
        time_Drop_Fields="0-06:00:00"
        ntasks_Drop_Fields=4
        mem_Drop_Fields="250000"

        partition_Morningstar_Build="<SLURM_PARTITION>"
        time_Morningstar_Build="0-04:00:00"
        ntasks_Morningstar_Build=4
        mem_Morningstar_Build="200000"

        partition_PortfolioSummary_Build="<SLURM_PARTITION>"
        time_PortfolioSummary_Build="0-03:00:00"
        ntasks_PortfolioSummary_Build=4
        mem_PortfolioSummary_Build="200000"

        partition_HoldingDetail_Build_Small="<SLURM_PARTITION>"
        time_HoldingDetail_Build_Small="0-05:00:00"
        ntasks_HoldingDetail_Build_Small=4
        mem_HoldingDetail_Build_Small="200000"

        partition_HoldingDetail_Build_Large="<SLURM_PARTITION>"
        time_HoldingDetail_Build_Large="0-12:00:00"
        ntasks_HoldingDetail_Build_Large=10
        mem_HoldingDetail_Build_Large="250000"

        partition_parse_externalid="<SLURM_PARTITION>"
        time_parse_externalid="0-3:00:00"
        ntasks_parse_externalid=4
        mem_parse_externalid="600000"
            
        partition_externalid="<SLURM_PARTITION>"
        time_externalid="0-6:00:00"
        ntasks_externalid=8
        mem_externalid="250000"

        partition_Data_Improvement="<SLURM_PARTITION>"
        time_Data_Improvement="0-4:00:00"
        ntasks_Data_Improvement=4
        mem_Data_Improvement="400000"

        partition_externalid_make="<SLURM_PARTITION>"
        time_externalid_make="0-29:00:00"
        ntasks_externalid_make=8
        mem_externalid_make="250000"

        partition_externalid_merge_Small="<SLURM_PARTITION>"
        time_externalid_merge_Small="0-4:00:00"
        ntasks_externalid_merge_Small=2
        mem_externalid_merge_Small="250000"

        partition_externalid_merge_Large="<SLURM_PARTITION>"
        time_externalid_merge_Large="0-4:00:00"
        ntasks_externalid_merge_Large=2
        mem_externalid_merge_Large="250000"

        partition_Intermediate_Corrections="<SLURM_PARTITION>"
        time_Intermediate_Corrections="0-4:00:00"
        ntasks_Intermediate_Corrections=4
        mem_Intermediate_Corrections="200000"

        partition_Fuzzy_Merge_Good_Data_Step11="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Good_Data_Step11="0-10:00:00"
        ntasks_Fuzzy_Merge_Good_Data_Step11=4
        mem_Fuzzy_Merge_Good_Data_Step11="62000"

        partition_Fuzzy_Merge_Good_Data_Step12="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Good_Data_Step12="0-10:00:00"
        ntasks_Fuzzy_Merge_Good_Data_Step12=12
        mem_Fuzzy_Merge_Good_Data_Step12="150000"

        partition_Fuzzy_Merge_Bad_Data_Step11="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Bad_Data_Step11="0-10:00:00"
        ntasks_Fuzzy_Merge_Bad_Data_Step11=4
        mem_Fuzzy_Merge_Bad_Data_Step11="62000"

        partition_Fuzzy_Merge_Bad_Data_Step12="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Bad_Data_Step12="0-10:00:00"
        ntasks_Fuzzy_Merge_Bad_Data_Step12=12
        mem_Fuzzy_Merge_Bad_Data_Step12="150000"

        partition_Fuzzy_Merge_Train_Linker="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Train_Linker="0-03:00:00"
        ntasks_Fuzzy_Merge_Train_Linker=4
        mem_Fuzzy_Merge_Train_Linker="90000"

        partition_Fuzzy_Merge_Find_Matches="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Find_Matches="0-05:00:00"
        ntasks_Fuzzy_Merge_Find_Matches=1
        mem_Fuzzy_Merge_Find_Matches="30000"
        num_shards_Fuzzy_Merge_Find_Matches=100
        array_Fuzzy_Merge_Find_Matches="0-99"
        mem_Fuzzy_Merge_Find_Matches_US_Bonds="62000"
        time_Fuzzy_Merge_Find_Matches_US_Bonds="0-10:00:00"

        partition_Fuzzy_Merge_Finalize="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Finalize="0-02:00:00"
        ntasks_Fuzzy_Merge_Finalize=1
        mem_Fuzzy_Merge_Finalize="60000"

        partition_Fuzzy_Merge_Step2="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Step2="0-02:00:00"
        ntasks_Fuzzy_Merge_Step2=4
        mem_Fuzzy_Merge_Step2="200000"

        partition_Fuzzy_Merge_Step3_Small="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Step3_Small="0-03:00:00"
        ntasks_Fuzzy_Merge_Step3_Small=4
        mem_Fuzzy_Merge_Step3_Small="62000"

        partition_Fuzzy_Merge_Step3_Large="<SLURM_PARTITION>"
        time_Fuzzy_Merge_Step3_Large="0-03:00:00"
        ntasks_Fuzzy_Merge_Step3_Large=8
        mem_Fuzzy_Merge_Step3_Large="250000"

        partition_Cusip_Build="<SLURM_PARTITION>"
        time_Cusip_Build="0-06:00:00"
        ntasks_Cusip_Build=4
        mem_Cusip_Build="200000"

        partition_Currency="<SLURM_PARTITION>"
        time_Currency="0-06:00:00"
        ntasks_Currency=4
        mem_Currency="200000"

        partition_Cusip_HoldingDetail_Merge_Small="<SLURM_PARTITION>"
        time_Cusip_HoldingDetail_Merge_Small="0-05:00:00"
        ntasks_Cusip_HoldingDetail_Merge_Small=4
        mem_Cusip_HoldingDetail_Merge_Small="62000"

        partition_Cusip_HoldingDetail_Merge_Large="<SLURM_PARTITION>"
        time_Cusip_HoldingDetail_Merge_Large="0-05:00:00"
        ntasks_Cusip_HoldingDetail_Merge_Large=4
        mem_Cusip_HoldingDetail_Merge_Large="62000"

        partition_Country_Prelim="<SLURM_PARTITION>"
        time_Country_Prelim="0-05:00:00"
        ntasks_Country_Prelim=4
        mem_Country_Prelim="200000"

        partition_Internal_Class="<SLURM_PARTITION>"
        time_Internal_Class="0-05:00:00"
        ntasks_Internal_Class=4
        mem_Internal_Class="200000"

        partition_Ultimate_Parent_Aggregation="<SLURM_PARTITION>"
        time_Ultimate_Parent_Aggregation="0-10:00:00"
        ntasks_Ultimate_Parent_Aggregation=4
        mem_Ultimate_Parent_Aggregation="200000"

        partition_Country_Merge="<SLURM_PARTITION>"
        time_Country_Merge="0-05:00:00"
        ntasks_Country_Merge=4
        mem_Country_Merge="250000"

        partition_Manual_Corrections="<SLURM_PARTITION>"
        time_Manual_Corrections="0-6:00:00"
        ntasks_Manual_Corrections=12
        mem_Manual_Corrections="200000"

        partition_Prepare_MF_Unwinding="<SLURM_PARTITION>"
        time_Prepare_MF_Unwinding="0-05:00:00"
        ntasks_Prepare_MF_Unwinding=4
        mem_Prepare_MF_Unwinding="250000"

        partition_Unwind_MF_Positions_Step1="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step1="0-10:00:00"
        ntasks_Unwind_MF_Positions_Step1=4
        mem_Unwind_MF_Positions_Step1="250000"

        partition_Unwind_MF_Positions_Step15="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step15="0-2:00:00"
        ntasks_Unwind_MF_Positions_Step15=4
        mem_Unwind_MF_Positions_Step15="200000"

        partition_Unwind_MF_Positions_Step2_Small="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step2_Small="0-10:00:00"
        ntasks_Unwind_MF_Positions_Step2_Small=4
        mem_Unwind_MF_Positions_Step2_Small="250000"

        partition_Unwind_MF_Positions_Step2_Large="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step2_Large="0-10:00:00"
        ntasks_Unwind_MF_Positions_Step2_Large=4
        mem_Unwind_MF_Positions_Step2_Large="500000"

        partition_Unwind_MF_Positions_Step3_Small="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step3_Small="0-10:00:00"
        ntasks_Unwind_MF_Positions_Step3_Small=4
        mem_Unwind_MF_Positions_Step3_Small="250000"

        partition_Unwind_MF_Positions_Step3_Large="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step3_Large="0-18:00:00"
        ntasks_Unwind_MF_Positions_Step3_Large=4
        mem_Unwind_MF_Positions_Step3_Large="500000"

        partition_Unwind_MF_Positions_Step4="<SLURM_PARTITION>"
        time_Unwind_MF_Positions_Step4="0-10:00:00"
        ntasks_Unwind_MF_Positions_Step4=4
        mem_Unwind_MF_Positions_Step4="250000"

        partition_Create_Final_Files="<SLURM_PARTITION>"
        time_Create_Final_Files="0-08:00:00"
        ntasks_Create_Final_Files=8
        mem_Create_Final_Files="200000"

        partition_Industry="<SLURM_PARTITION>"
        time_Industry="0-08:00:00"
        ntasks_Industry=8
        mem_Industry="200000"

    ;;

  *)

    echo "Invalid User: Exiting..."

    exitcd

    ;;

esac


# Export path variables
echo "Code path = "${mns_code_path}
echo "Data path = "${mns_data_path}
export mns_code_path
export mns_data_path

# Sanity-checking
if [[ -z "$mns_data_path" ]]; then
   echo "Empty mns_data_path: Exiting..."
fi
if [[ -z "$mns_code_path" ]]; then
   echo "Empty mns_code_path: Exiting..."
fi

# Get_Started: Clear the contents of the output and temporary folder
JOB_Get_Started_ID=`sbatch \
         --partition=${partition_Get_Started} --time=${time_Get_Started} \
         --nodes=${nodes} --ntasks=${ntasks_Get_Started} --job-name=Get_Started \
         --output="${mns_erroroutput_path}/Get_Started-%A_%a.out" --error="${mns_erroroutput_path}/Get_Started-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Get_Started} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Get_Started | awk '{print $NF}'`
echo "Submitted Get_Started Job: "${JOB_Get_Started_ID}
sleep 1


# Run raw XML import if necessary
if [ ${run_xml_import} = 1 ]; then

    echo "Running XML import steps"

    # XML_Import: Runs the SAS job that creates DTA files
    # Note that memory/node/etc. settings for these are set separately, using the scripts in the xmlimport folder
    # Please remember to code in the number of 7z files in each directory when adding new folders (see process_directory_slurm_array.sh)
    JOB_XML_Import_ID=`sh "${mns_code_path}/build/xmlimport/process_directory_slurm_array.sh" ${mns_data_path}/raw/morningstar_ftp_master`
    echo "Submitted XML_Import Job: "${JOB_XML_Import_ID}

    Drop_Fields_Dependency_ID=$JOB_XML_Import_ID

else

    Drop_Fields_Dependency_ID=$JOB_Get_Started_ID

fi


# Drop_Fields: Drop unnecessary fields from the extracted raw Morningstar holdings data
array_Drop_Fields="1-33"
JOB_Drop_Fields_ID=`sbatch \
         --partition=${partition_Drop_Fields} --time=${time_Drop_Fields} \
         --nodes=${nodes} --ntasks=${ntasks_Drop_Fields} --job-name=Drop_Fields \
          --array=${array_Drop_Fields}  --output="${mns_erroroutput_path}/Drop_Fields-%A_%a.out" --error="${mns_erroroutput_path}/Drop_Fields-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${Drop_Fields_Dependency_ID} --mem=${mem_Drop_Fields} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Drop_Fields | awk '{print $NF}'`
echo "Submitted Drop_Fields Job: "${JOB_Drop_Fields_ID}
sleep 1


# Public_Portfolios_Build: Builds data from TIC, ICI, CPIS, and OECD
JOB_Public_Portfolios_Build_ID=`sbatch \
         --partition=${partition_Public_Portfolios_Build} --time=${time_Public_Portfolios_Build} \
         --nodes=${nodes} --ntasks=${ntasks_Public_Portfolios_Build} --job-name=Public_Portfolios_Build \
         --output="${mns_erroroutput_path}/Public_Portfolios_Build-%A_%a.out" --error="${mns_erroroutput_path}/Public_Portfolios_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Public_Portfolios_Build} \
         --depend=afterok:${JOB_Get_Started_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Public_Portfolios_Build | awk '{print $NF}'`
echo "Submitted Public_Portfolios_Build Job: "${JOB_Public_Portfolios_Build_ID}
sleep 1


# Cusip_Build: Build the security and issuer master files from CUSIP Global Services (CGS)
JOB_Cusip_Build_ID=`sbatch \
         --partition=${partition_Cusip_Build} --time=${time_Cusip_Build} \
         --nodes=${nodes} --ntasks=${ntasks_Cusip_Build} --job-name=Cusip_Build \
          --output="${mns_erroroutput_path}/Cusip_Build-%A_%a.out" --error="${mns_erroroutput_path}/Cusip_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Get_Started_ID} --mem=${mem_Cusip_Build} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Cusip_Build | awk '{print $NF}'`
echo "Submitted Cusip_Build Job: "${JOB_Cusip_Build_ID}
sleep 1


# Macro_Build (Steps 1 and 2): Clean and build macroeconomic data (e.g. exchange rates) from various sources
array_Macro_Build_Step1="1-25"
JOB_Macro_Build_Step1_ID=`sbatch \
         --partition=${partition_Macro_Build_Step1} --time=${time_Macro_Build_Step1} \
         --nodes=${nodes} --ntasks=${ntasks_Macro_Build_Step1} --job-name=Macro_Build_Step1 \
         --array=${array_Macro_Build_Step1} --output="${mns_erroroutput_path}/Macro_Build_Step1-%A_%a.out" --error="${mns_erroroutput_path}/Macro_Build_Step1-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Macro_Build_Step1} \
         --depend=afterok:${JOB_Get_Started_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Macro_Build_Step1 | awk '{print $NF}'`
echo "Submitted Macro_Build_Step1 Job: "${JOB_Macro_Build_Step1_ID}
sleep 1

JOB_Macro_Build_Step2_ID=`sbatch \
         --partition=${partition_Macro_Build_Step2} --time=${time_Macro_Build_Step2} \
         --nodes=${nodes} --ntasks=${ntasks_Macro_Build_Step2} --job-name=Macro_Build_Step2 \
         --output="${mns_erroroutput_path}/Macro_Build_Step2-%A_%a.out" --error="${mns_erroroutput_path}/Macro_Build_Step2-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
          --depend=afterok:${JOB_Macro_Build_Step1_ID} --mem=${mem_Macro_Build_Step2} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Macro_Build_Step2 | awk '{print $NF}'`
echo "Submitted Macro_Build_Step2 Job: "${JOB_Macro_Build_Step2_ID}
sleep 1


# Orbis_Build (Steps 1, 2, and 3): Build the ownership data from Bureau van Dijk's ORBIS database
JOB_Orbis_Build_Step1_ID=`sbatch \
         --partition=${partition_Orbis_Build_Step1} --time=${time_Orbis_Build_Step1} \
         --nodes=${nodes} --ntasks=${ntasks_Orbis_Build_Step1} --job-name=Orbis_Build_Step1 \
         --output="${mns_erroroutput_path}/Orbis_Build_Step1-%A_%a.out" --error="${mns_erroroutput_path}/Orbis_Build_Step1-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Orbis_Build_Step1} \
         --depend=afterok:${JOB_Cusip_Build_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Orbis_Build_Step1 | awk '{print $NF}'`
echo "Submitted Orbis_Build_Step1 Job: "${JOB_Orbis_Build_Step1_ID}
sleep 1

array_Orbis_Build_Step2="1-225"
JOB_Orbis_Build_Step2_ID=`sbatch \
         --partition=${partition_Orbis_Build_Step2} --time=${time_Orbis_Build_Step2} \
         --nodes=${nodes} --ntasks=${ntasks_Orbis_Build_Step2} --job-name=Orbis_Build_Step2 \
         --output="${mns_erroroutput_path}/Orbis_Build_Step2-%A_%a.out" --error="${mns_erroroutput_path}/Orbis_Build_Step2-%A_%a.err" \
         --array=${array_Orbis_Build_Step2} --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Orbis_Build_Step1_ID} --mem=${mem_Orbis_Build_Step2} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Orbis_Build_Step2 | awk '{print $NF}'`
echo "Submitted Orbis_Build_Step2 Job: "${JOB_Orbis_Build_Step2_ID}
sleep 1

JOB_Orbis_Build_Step3_ID=`sbatch \
         --partition=${partition_Orbis_Build_Step3} --time=${time_Orbis_Build_Step3} \
         --nodes=${nodes} --ntasks=${ntasks_Orbis_Build_Step3} --job-name=Orbis_Build_Step3 \
         --output="${mns_erroroutput_path}/Orbis_Build_Step3-%A_%a.out" --error="${mns_erroroutput_path}/Orbis_Build_Step3-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Orbis_Build_Step3} \
         --depend=afterok:${JOB_Orbis_Build_Step2_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Orbis_Build_Step3 | awk '{print $NF}'`
echo "Submitted Orbis_Build_Step3 Job: "${JOB_Orbis_Build_Step3_ID}
sleep 1

# Dealogic_Build: Build issue-level bond data from Dealogic
JOB_Dealogic_Build_ID=`sbatch \
         --partition=${partition_Dealogic_Build} --time=${time_Dealogic_Build} \
         --nodes=${nodes} --ntasks=${ntasks_Dealogic_Build} --job-name=Dealogic_Build \
         --output="${mns_erroroutput_path}/Dealogic_Build-%A_%a.out" --error="${mns_erroroutput_path}/Dealogic_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Dealogic_Build} \
         --depend=afterok:${JOB_Cusip_Build_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Dealogic_Build | awk '{print $NF}'`
echo "Submitted Dealogic_Build Job: "${JOB_Dealogic_Build_ID}
sleep 1

# Morningstar_Build: Build accompanying metadata (especially fund-level information) from Morningstar
JOB_Morningstar_Build_ID=`sbatch \
         --partition=${partition_Morningstar_Build} --time=${time_Morningstar_Build} \
         --nodes=${nodes} --ntasks=${ntasks_Morningstar_Build} --job-name=Morningstar_Build \
         --output="${mns_erroroutput_path}/Morningstar_Build-%A_%a.out" --error="${mns_erroroutput_path}/Morningstar_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Drop_Fields_ID} --mem=${mem_Morningstar_Build} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Morningstar_Build | awk '{print $NF}'`
echo "Submitted Morningstar_Build Job: "${JOB_Morningstar_Build_ID}
sleep 1


# PortfolioSummary_Build: Generate a clean dataset with portfolio summary data
JOB_PortfolioSummary_Build_ID=`sbatch \
         --partition=${partition_PortfolioSummary_Build} --time=${time_PortfolioSummary_Build} \
         --nodes=${nodes} --ntasks=${ntasks_PortfolioSummary_Build} --job-name=PortfolioSummary_Build \
         --output="${mns_erroroutput_path}/PortfolioSummary_Build-%A_%a.out" --error="${mns_erroroutput_path}/PortfolioSummary_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --mem=${mem_PortfolioSummary_Build} --requeue \
        --depend=afterok:${JOB_Macro_Build_Step2_ID}:${JOB_Morningstar_Build_ID}  \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} PortfolioSummary_Build | awk '{print $NF}'`
echo "Submitted PortfolioSummary_Build Job: "${JOB_PortfolioSummary_Build_ID}
sleep 1

# HoldingDetail_Build: Read in the HoldingDetail files, clean and append them, and then merge with API and FX data
array_HoldingDetail_Build_Small="1-12"
JOB_HoldingDetail_Build_Small_ID=`sbatch \
         --partition=${partition_HoldingDetail_Build_Small} --time=${time_HoldingDetail_Build_Small} \
         --nodes=${nodes} --ntasks=${ntasks_HoldingDetail_Build_Small} --job-name=HoldingDetail_Build_Small \
          --array=${array_HoldingDetail_Build_Small} --output="${mns_erroroutput_path}/HoldingDetail_Build_Small-%A_%a.out" --error="${mns_erroroutput_path}/HoldingDetail_Build_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Macro_Build_Step2_ID}:${JOB_Morningstar_Build_ID} --mem=${mem_HoldingDetail_Build_Small} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} HoldingDetail_Build | awk '{print $NF}'`
echo "Submitted HoldingDetail_Build_Small Job: "${JOB_HoldingDetail_Build_Small_ID}
sleep 1

array_HoldingDetail_Build_Large="13-33"
JOB_HoldingDetail_Build_Large_ID=`sbatch \
         --partition=${partition_HoldingDetail_Build_Large} --time=${time_HoldingDetail_Build_Large} \
         --nodes=${nodes} --ntasks=${ntasks_HoldingDetail_Build_Large} --job-name=HoldingDetail_Build_Large \
          --array=${array_HoldingDetail_Build_Large} --output="${mns_erroroutput_path}/HoldingDetail_Build_Large-%A_%a.out" --error="${mns_erroroutput_path}/HoldingDetail_Build_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Macro_Build_Step2_ID}:${JOB_Morningstar_Build_ID} --mem=${mem_HoldingDetail_Build_Large} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} HoldingDetail_Build | awk '{print $NF}'`
echo "Submitted HoldingDetail_Build_Large Job: "${JOB_HoldingDetail_Build_Large_ID}
sleep 1

# Parse external id: Clean and parse the externalid field in the Morningstar holdings data; this will
# be used in conjunction with the OpenFIGI API in order to identify securities for which we are otherwise 
# lacking identifiers
array_parse_externalid="1-33"
JOB_parse_externalid_ID=`sbatch \
         --partition=${partition_parse_externalid} --time=${time_parse_externalid} \
         --nodes=${nodes} --ntasks=${ntasks_parse_externalid} --job-name=parse_externalid \
          --array=${array_parse_externalid} --output="${mns_erroroutput_path}/parse_externalid-%A_%a.out" --error="${mns_erroroutput_path}/parse_externalid-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_HoldingDetail_Build_Large_ID}:${JOB_HoldingDetail_Build_Small_ID} --mem=${mem_parse_externalid} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} parse_externalid | awk '{print $NF}'`
echo "Submitted parse_externalid Job: "${JOB_parse_externalid_ID}
sleep 1


# externalid: Consolidate the list of externalids to be sent to OpenFIGI via API; query identifiers via
# the OpenFIGI API, and obtain corresponding CUSIP/ISIN identifiers via Bloomberg
JOB_externalid_ID=`sbatch \
         --partition=${partition_externalid} --time=${time_externalid} \
         --nodes=${nodes} --ntasks=${ntasks_externalid} --job-name=externalid \
        --output="${mns_erroroutput_path}/externalid-%A_%a.out" --error="${mns_erroroutput_path}/externalid-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_parse_externalid_ID} --mem=${mem_externalid} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} externalid | awk '{print $NF}'`
echo "Submitted externalid Job: "${JOB_externalid_ID}
sleep 1


# Data_Improvement: Perform a series of data cleaning steps that improve the quality of security metadata 
# by merging in information from the CGS security master files and the OpenFIGI/Bloomberg data pull
array_Data_Improvement="1-33"
JOB_Data_Improvement_ID=`sbatch \
         --partition=${partition_Data_Improvement} --time=${time_Data_Improvement} \
         --nodes=${nodes} --ntasks=${ntasks_Data_Improvement} --job-name=Data_Improvement \
          --array=${array_Data_Improvement} --output="${mns_erroroutput_path}/Data_Improvement-%A_%a.out" --error="${mns_erroroutput_path}/Data_Improvement-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_externalid_ID} --mem=${mem_Data_Improvement} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Data_Improvement | awk '{print $NF}'`
echo "Submitted Data_Improvement Job: "${JOB_Data_Improvement_ID}
sleep 1


# externalid_make: Create an internal flatfile which has all security-level details for each externalid in the Morningstar holdings data
JOB_externalid_make_ID=`sbatch \
         --partition=${partition_externalid_make} --time=${time_externalid_make} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_make} --job-name=externalid_make \
        --output="${mns_erroroutput_path}/externalid_make-%A_%a.out" --error="${mns_erroroutput_path}/externalid_make-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Data_Improvement_ID} --mem=${mem_externalid_make} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} externalid_make | awk '{print $NF}'`
echo "Submitted externalid_make Job: "${JOB_externalid_make_ID}
sleep 1


# externalid_merge: Merge information from the internally-generated externalid master file into the holdings data
array_externalid_merge_Small="1-25"
JOB_externalid_merge_Small_ID=`sbatch \
         --partition=${partition_externalid_merge_Small} --time=${time_externalid_merge_Small} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_merge_Small} --job-name=externalid_merge_Small \
          --array=${array_externalid_merge_Small} --output="${mns_erroroutput_path}/externalid_merge_Small-%A_%a.out" --error="${mns_erroroutput_path}/externalid_merge_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_externalid_make_ID} --mem=${mem_externalid_merge_Small} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} externalid_merge | awk '{print $NF}'`
echo "Submitted externalid_merge_Small Job: "${JOB_externalid_merge_Small_ID}
sleep 1

array_externalid_merge_Large="26-33"
JOB_externalid_merge_Large_ID=`sbatch \
         --partition=${partition_externalid_merge_Large} --time=${time_externalid_merge_Large} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_merge_Large} --job-name=externalid_merge_Large \
          --array=${array_externalid_merge_Large} --output="${mns_erroroutput_path}/externalid_merge_Large-%A_%a.out" --error="${mns_erroroutput_path}/externalid_merge_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_externalid_make_ID} --mem=${mem_externalid_merge_Large} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} externalid_merge | awk '{print $NF}'`
echo "Submitted externalid_merge_Large Job: "${JOB_externalid_merge_Large_ID}
sleep 1

# The 'fuzzy merge' steps below handle the probabilistic record linkage of observations in the 
# Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
# do have an identifier.

# Fuzzy_Merge_Good_Data, Step 11: Clean and organize the security records for which a CUSIP is available
array_Fuzzy_Merge_Good_Data_Step11="1-33"
JOB_Fuzzy_Merge_Good_Data_Step11_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Good_Data_Step11} --time=${time_Fuzzy_Merge_Good_Data_Step11} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Good_Data_Step11} --job-name=Fuzzy_Merge_Good_Data_Step11 \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Good_Data_Step11-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Good_Data_Step11-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_externalid_merge_Small_ID}:${JOB_externalid_merge_Large_ID} \
         --mem=${mem_Fuzzy_Merge_Good_Data_Step11} \
         --array=${array_Fuzzy_Merge_Good_Data_Step11} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Good_Data_Step11 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Good_Data_Step11 Job: "${JOB_Fuzzy_Merge_Good_Data_Step11_ID}
sleep 1


# Fuzzy_Merge_Good_Data, Step 12: Clean and organize the security records for which a CUSIP is available
JOB_Fuzzy_Merge_Good_Data_Step12_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Good_Data_Step12} --time=${time_Fuzzy_Merge_Good_Data_Step12} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Good_Data_Step12} --job-name=Fuzzy_Merge_Good_Data_Step12 \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Good_Data_Step12-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Good_Data_Step12-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Good_Data_Step11_ID} \
         --mem=${mem_Fuzzy_Merge_Good_Data_Step12} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Good_Data_Step12 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Good_Data_Step12 Job: "${JOB_Fuzzy_Merge_Good_Data_Step12_ID}
sleep 1


# Fuzzy_Merge_Bad_Data, Step 11: Clean and organize the security records for which a CUSIP is not available
array_Fuzzy_Merge_Bad_Data_Step11="1-33"
JOB_Fuzzy_Merge_Bad_Data_Step11_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Bad_Data_Step11} --time=${time_Fuzzy_Merge_Bad_Data_Step11} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Bad_Data_Step11} --job-name=Fuzzy_Merge_Bad_Data_Step11 \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Bad_Data_Step11-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Bad_Data_Step11-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_externalid_merge_Small_ID}:${JOB_externalid_merge_Large_ID} \
         --mem=${mem_Fuzzy_Merge_Bad_Data_Step11} \
         --array=${array_Fuzzy_Merge_Bad_Data_Step11} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Bad_Data_Step11 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Bad_Data_Step11 Job: "${JOB_Fuzzy_Merge_Bad_Data_Step11_ID}
sleep 1


# Fuzzy_Merge_Bad_Data, Step 12: Clean and organize the security records for which a CUSIP is not available
JOB_Fuzzy_Merge_Bad_Data_Step12_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Bad_Data_Step12} --time=${time_Fuzzy_Merge_Bad_Data_Step12} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Bad_Data_Step12} --job-name=Fuzzy_Merge_Bad_Data_Step12 \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Bad_Data_Step12-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Bad_Data_Step12-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Bad_Data_Step11_ID} \
         --mem=${mem_Fuzzy_Merge_Bad_Data_Step12} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Bad_Data_Step12 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Bad_Data_Step12 Job: "${JOB_Fuzzy_Merge_Bad_Data_Step12_ID}
sleep 1


# The Fuzzy_Merge_Train_Linker steps train a regularized logistic regression model to generate pairwise 
# match probabilities for the security records.

# Fuzzy_Merge_Train_Linker; Bonds
ASSET_CLASS=bonds
NUM_SHARDS=${num_shards_Fuzzy_Merge_Find_Matches}
export ASSET_CLASS
export NUM_SHARDS
JOB_Fuzzy_Merge_Train_Linker_Bonds_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Train_Linker} --time=${time_Fuzzy_Merge_Train_Linker} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Train_Linker} --job-name=Fuzzy_Merge_Train_Linker_Bonds \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Train_Linker_Bonds-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Train_Linker_Bonds-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Good_Data_Step12_ID}:${JOB_Fuzzy_Merge_Bad_Data_Step12_ID} \
         --mem=${mem_Fuzzy_Merge_Train_Linker} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Train_Linker | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Train_Linker_Bonds Job: "${JOB_Fuzzy_Merge_Train_Linker_Bonds_ID}
sleep 1


# Fuzzy_Merge_Train_Linker; Stocks
ASSET_CLASS=stocks
NUM_SHARDS=${num_shards_Fuzzy_Merge_Find_Matches}
export ASSET_CLASS
export NUM_SHARDS
JOB_Fuzzy_Merge_Train_Linker_Stocks_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Train_Linker} --time=${time_Fuzzy_Merge_Train_Linker} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Train_Linker} --job-name=Fuzzy_Merge_Train_Linker_Stocks \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Train_Linker_Stocks-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Train_Linker_Stocks-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Good_Data_Step12_ID}:${JOB_Fuzzy_Merge_Bad_Data_Step12_ID} \
         --mem=${mem_Fuzzy_Merge_Train_Linker} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Train_Linker | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Train_Linker_Stocks Job: "${JOB_Fuzzy_Merge_Train_Linker_Stocks_ID}
sleep 1


# The Fuzzy_Merge_Find_Matches steps compute the pairwise match probabilities and store the matched data to disk

# Fuzzy_Merge_Find_Matches; US bonds
ASSET_CLASS=bonds
GEOGRAPHY=us
DO_FULL_PASS=1
SKIP_MATCHED=0
export ASSET_CLASS
export GEOGRAPHY
export DO_FULL_PASS
export SKIP_MATCHED
JOB_Fuzzy_Merge_Find_Matches_Bonds_US_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Find_Matches} --time=${time_Fuzzy_Merge_Find_Matches_US_Bonds} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Find_Matches} --job-name=Fuzzy_Merge_Find_Matches_Bonds_US \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_US-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_US-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --array=${array_Fuzzy_Merge_Find_Matches} \
         --depend=afterok:${JOB_Fuzzy_Merge_Train_Linker_Bonds_ID}  \
         --mem=${mem_Fuzzy_Merge_Find_Matches_US_Bonds} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Find_Matches | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Find_Matches_Bonds_US Job: "${JOB_Fuzzy_Merge_Find_Matches_Bonds_US_ID}
sleep 1


# Fuzzy_Merge_Find_Matches; NonUS bonds
ASSET_CLASS=bonds
GEOGRAPHY=nonus
DO_FULL_PASS=1
SKIP_MATCHED=0
export ASSET_CLASS
export GEOGRAPHY
export DO_FULL_PASS
export SKIP_MATCHED
JOB_Fuzzy_Merge_Find_Matches_Bonds_NonUS_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Find_Matches} --time=${time_Fuzzy_Merge_Find_Matches} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Find_Matches} --job-name=Fuzzy_Merge_Find_Matches_Bonds_NonUS \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_NonUS-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_NonUS-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --array=${array_Fuzzy_Merge_Find_Matches} \
         --depend=afterok:${JOB_Fuzzy_Merge_Train_Linker_Bonds_ID}  \
         --mem=${mem_Fuzzy_Merge_Find_Matches} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Find_Matches | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Find_Matches_Bonds_NonUS Job: "${JOB_Fuzzy_Merge_Find_Matches_Bonds_NonUS_ID}
sleep 1


# Fuzzy_Merge_Find_Matches; Residual bonds
ASSET_CLASS=bonds
GEOGRAPHY=all
DO_FULL_PASS=0
SKIP_MATCHED=1
export ASSET_CLASS
export GEOGRAPHY
export DO_FULL_PASS
export SKIP_MATCHED
JOB_Fuzzy_Merge_Find_Matches_Bonds_Residual_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Find_Matches} --time=${time_Fuzzy_Merge_Find_Matches} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Find_Matches} --job-name=Fuzzy_Merge_Find_Matches_Bonds_Residual \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_Residual-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Bonds_Residual-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --array=${array_Fuzzy_Merge_Find_Matches} \
         --depend=afterok:${JOB_Fuzzy_Merge_Find_Matches_Bonds_NonUS_ID}:${JOB_Fuzzy_Merge_Find_Matches_Bonds_US_ID} \
         --mem=${mem_Fuzzy_Merge_Find_Matches} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Find_Matches | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Find_Matches_Bonds_Residual Job: "${JOB_Fuzzy_Merge_Find_Matches_Bonds_Residual_ID}
sleep 1


# Fuzzy_Merge_Find_Matches; Stocks
ASSET_CLASS=stocks
GEOGRAPHY=all
DO_FULL_PASS=1
SKIP_MATCHED=0
export ASSET_CLASS
export GEOGRAPHY
export DO_FULL_PASS
export SKIP_MATCHED
JOB_Fuzzy_Merge_Find_Matches_Stocks_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Find_Matches} --time=${time_Fuzzy_Merge_Find_Matches} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Find_Matches} --job-name=Fuzzy_Merge_Find_Matches_Stocks \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Stocks-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Find_Matches_Stocks-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --array=${array_Fuzzy_Merge_Find_Matches} \
         --depend=afterok:${JOB_Fuzzy_Merge_Train_Linker_Stocks_ID} \
         --mem=${mem_Fuzzy_Merge_Find_Matches} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Find_Matches | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Find_Matches_Stocks Job: "${JOB_Fuzzy_Merge_Find_Matches_Stocks_ID}
sleep 1


# The Fuzzy_Merge_Finalize steps collect the shards of matched data generated by the probabilistic record linker
# in the Fuzzy_Merge_Find_Matches jobs, concatenate them, and store them to disk

# Fuzzy_Merge_Finalize; Bonds
ASSET_CLASS=bonds
export ASSET_CLASS
JOB_Fuzzy_Merge_Finalize_Bonds_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Finalize} --time=${time_Fuzzy_Merge_Finalize} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Finalize} --job-name=Fuzzy_Merge_Finalize_Bonds \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Finalize_Bonds-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Finalize_Bonds-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Find_Matches_Bonds_US_ID}:${JOB_Fuzzy_Merge_Find_Matches_Bonds_NonUS_ID}:${JOB_Fuzzy_Merge_Find_Matches_Bonds_Residual_ID} \
         --mem=${mem_Fuzzy_Merge_Finalize} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Finalize | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Finalize_Bonds Job: "${JOB_Fuzzy_Merge_Finalize_Bonds_ID}
sleep 1


# Fuzzy_Merge_Finalize; Stocks
ASSET_CLASS=stocks
export ASSET_CLASS
JOB_Fuzzy_Merge_Finalize_Stocks_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Finalize} --time=${time_Fuzzy_Merge_Finalize} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Finalize} --job-name=Fuzzy_Merge_Finalize_Stocks \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Finalize_Stocks-%A_%a.out" \
         --error="${mns_erroroutput_path}/Fuzzy_Merge_Finalize_Stocks-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Find_Matches_Stocks_ID}  \
         --mem=${mem_Fuzzy_Merge_Finalize} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Fuzzy_Merge_Finalize | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Finalize_Stocks Job: "${JOB_Fuzzy_Merge_Finalize_Stocks_ID}
sleep 1


# Fuzzy_Merge_Step2: Build an integrated dataset from the matched subsamples obtained via probabilistic record linkage
JOB_Fuzzy_Merge_Step2_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Step2} --time=${time_Fuzzy_Merge_Step2} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Step2} --job-name=Fuzzy_Merge_Step2 \
         --output="${mns_erroroutput_path}/Fuzzy_Merge_Step2-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Step2-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Fuzzy_Merge_Step2} \
         --depend=afterok:${JOB_Fuzzy_Merge_Finalize_Stocks_ID}:${JOB_Fuzzy_Merge_Finalize_Bonds_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Step2 ${array_Fuzzy_Merge_Step1s} | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Step2 Job: "${JOB_Fuzzy_Merge_Step2_ID}
sleep 1


# Fuzzy_Merge_Step3: Reintroduce the probabilistic matches into the HoldingDetail files
array_Fuzzy_Merge_Step3_Small="1-23"
JOB_Fuzzy_Merge_Step3_Small_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Step3_Small} --time=${time_Fuzzy_Merge_Step3_Small} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Step3_Small} --job-name=Fuzzy_Merge_Step3_Small \
         --array=${array_Fuzzy_Merge_Step3_Small} --output="${mns_erroroutput_path}/Fuzzy_Merge_Step3_Small-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Step3_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Fuzzy_Merge_Step3_Small} \
         --depend=afterok:${JOB_Fuzzy_Merge_Step2_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Step3 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Step3_Small Job: "${JOB_Fuzzy_Merge_Step3_Small_ID}
sleep 1

array_Fuzzy_Merge_Step3_Large="24-33"
JOB_Fuzzy_Merge_Step3_Large_ID=`sbatch \
         --partition=${partition_Fuzzy_Merge_Step3_Large} --time=${time_Fuzzy_Merge_Step3_Large} \
         --nodes=${nodes} --ntasks=${ntasks_Fuzzy_Merge_Step3_Large} --job-name=Fuzzy_Merge_Step3_Large \
         --array=${array_Fuzzy_Merge_Step3_Large} --output="${mns_erroroutput_path}/Fuzzy_Merge_Step3_Large-%A_%a.out" --error="${mns_erroroutput_path}/Fuzzy_Merge_Step3_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Fuzzy_Merge_Step3_Large} \
         --depend=afterok:${JOB_Fuzzy_Merge_Step2_ID} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Fuzzy_Merge_Step3 | awk '{print $NF}'`
echo "Submitted Fuzzy_Merge_Step3_Large Job: "${JOB_Fuzzy_Merge_Step3_Large_ID}
sleep 1


# Currency: Construct a dataset with the modal currency assignments for each security within the Morningstar holdings data
JOB_Currency_ID=`sbatch \
         --partition=${partition_Currency} --time=${time_Currency} \
         --nodes=${nodes} --ntasks=${ntasks_Currency} --job-name=Currency \
          --output="${mns_erroroutput_path}/Currency-%A_%a.out" --error="${mns_erroroutput_path}/Currency--%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Fuzzy_Merge_Step3_Small_ID}:${JOB_Fuzzy_Merge_Step3_Large_ID} --mem=${mem_Currency} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Currency | awk '{print $NF}'`
echo "Submitted Currency Job: "${JOB_Currency_ID}
sleep 1

# Cusip_HoldingDetail_Merge: Merge in security-level data from the CUSIP Global Services (CGS) master files into the holdings data
array_Cusip_HoldingDetail_Merge_Small="1-25"
JOB_Cusip_HoldingDetail_Merge_Small_ID=`sbatch \
         --partition=${partition_Cusip_HoldingDetail_Merge_Small} --time=${time_Cusip_HoldingDetail_Merge_Small} \
         --nodes=${nodes} --ntasks=${ntasks_Cusip_HoldingDetail_Merge_Small} --job-name=Cusip_HoldingDetail_Merge_Small \
         --array=${array_Cusip_HoldingDetail_Merge_Small}   --output="${mns_erroroutput_path}/Cusip_HoldingDetail_Merge_Small-%A_%a.out" --error="${mns_erroroutput_path}/Cusip_HoldingDetail_Merge_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Currency_ID}:${JOB_Cusip_Build_ID} --mem=${mem_Cusip_HoldingDetail_Merge_Small} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Cusip_HoldingDetail_Merge | awk '{print $NF}'`
echo "Submitted Cusip_HoldingDetail_Merge_Small Job: "${JOB_Cusip_HoldingDetail_Merge_Small_ID}
sleep 1

array_Cusip_HoldingDetail_Merge_Large="26-40"
JOB_Cusip_HoldingDetail_Merge_Large_ID=`sbatch \
         --partition=${partition_Cusip_HoldingDetail_Merge_Large} --time=${time_Cusip_HoldingDetail_Merge_Large} \
         --nodes=${nodes} --ntasks=${ntasks_Cusip_HoldingDetail_Merge_Large} --job-name=Cusip_HoldingDetail_Merge_Large \
          --array=${array_Cusip_HoldingDetail_Merge_Large}   --output="${mns_erroroutput_path}/Cusip_HoldingDetail_Merge_Large-%A_%a.out" --error="${mns_erroroutput_path}/Cusip_HoldingDetail_Merge_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Currency_ID}:${JOB_Cusip_Build_ID} --mem=${mem_Cusip_HoldingDetail_Merge_Large} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Cusip_HoldingDetail_Merge | awk '{print $NF}'`
echo "Submitted Cusip_HoldingDetail_Merge_Large Job: "${JOB_Cusip_HoldingDetail_Merge_Large_ID}
sleep 1

# Internal_Class: Construct a dataset with the modal typecode assignments for each security within the Morningstar holdings data
JOB_Internal_Class_ID=`sbatch \
         --partition=${partition_Internal_Class} --time=${time_Internal_Class} \
         --nodes=${nodes} --ntasks=${ntasks_Internal_Class} --job-name=Internal_Class \
          --output="${mns_erroroutput_path}/Internal_Class-%A_%a.out" --error="${mns_erroroutput_path}/Internal_Class--%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Cusip_HoldingDetail_Merge_Small_ID}:${JOB_Cusip_HoldingDetail_Merge_Large_ID} --mem=${mem_Internal_Class} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Internal_Class | awk '{print $NF}'`
echo "Submitted Internal_Class Job: "${JOB_Internal_Class_ID}
sleep 1

# Country_Prelim: Run a series of data builds that prepare data used in the ultimate parent aggregation procedure
JOB_Country_Prelim_ID=`sbatch \
         --partition=${partition_Country_Prelim} --time=${time_Country_Prelim} \
         --nodes=${nodes} --ntasks=${ntasks_Country_Prelim} --job-name=Country_Prelim \
        --output="${mns_erroroutput_path}/Country_Prelim-%A_%a.out" --error="${mns_erroroutput_path}/Country_Prelim-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Cusip_HoldingDetail_Merge_Small_ID}:${JOB_Cusip_HoldingDetail_Merge_Large_ID} --mem=${mem_Country_Prelim} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Country_Prelim | awk '{print $NF}'`
echo "Submitted Country_Prelim Job: "${JOB_Country_Prelim_ID}
sleep 1

# Ultimate_Parent_Aggregation: Perform the ultimate-parent aggregation procedure described in Coppola, Maggiori, Neiman, and Schreger (2019)
JOB_Ultimate_Parent_Aggregation_ID=`sbatch \
         --partition=${partition_Ultimate_Parent_Aggregation} --time=${time_Ultimate_Parent_Aggregation} \
         --nodes=${nodes} --ntasks=${ntasks_Ultimate_Parent_Aggregation} --job-name=Ultimate_Parent_Aggregation \
        --output="${mns_erroroutput_path}/Ultimate_Parent_Aggregation-%A_%a.out" --error="${mns_erroroutput_path}/Ultimate_Parent_Aggregation-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Country_Prelim_ID}:{JOB_Orbis_Build_Step3_ID}:{JOB_Dealogic_Build_ID} \
         --mem=${mem_Ultimate_Parent_Aggregation} \
           "${mns_code_path}/build/Python_Controller.sh" ${U} Ultimate_Parent_Aggregation | awk '{print $NF}'`
echo "Submitted Ultimate_Parent_Aggregation Job: "${JOB_Ultimate_Parent_Aggregation_ID}
sleep 1


# Country_Merge: Merge in the ultimate-parent and country assignments from the ultimate-parent aggregation procedure
array_Country_Merge="1-33"
JOB_Country_Merge_ID=`sbatch \
         --partition=${partition_Country_Merge} --time=${time_Country_Merge} \
         --nodes=${nodes} --ntasks=${ntasks_Country_Merge} --job-name=Country_Merge \
          --output="${mns_erroroutput_path}/Country_Merge-%A_%a.out" --error="${mns_erroroutput_path}/Country_Merge-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Ultimate_Parent_Aggregation_ID} --mem=${mem_Country_Merge} --array=${array_Country_Merge} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Country_Merge | awk '{print $NF}'`
echo "Submitted Country_Merge Job: "${JOB_Country_Merge_ID}
sleep 1


# Manual_Corrections: Implement a number of manual correction to the holding data, which address outliers 
# and mistaken reporting in the raw Morningstar data
JOB_Manual_Corrections_ID=`sbatch \
         --partition=${partition_Manual_Corrections} --time=${time_Manual_Corrections} \
         --nodes=${nodes} --ntasks=${ntasks_Manual_Corrections} --job-name=Manual_Corrections \
          --output="${mns_erroroutput_path}/Manual_Corrections-%A_%a.out" --error="${mns_erroroutput_path}/Manual_Corrections-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Country_Merge_ID} \
         --mem=${mem_Manual_Corrections} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Manual_Corrections | awk '{print $NF}'`
echo "Submitted Manual_Corrections Job: "${JOB_Manual_Corrections_ID}
sleep 1


# The 'Unwind_MF' steps above handle the unwinding of funds' positions in other funds. 
# This procedure is referred to as fund-in-fund unwinding.

# Prepare_MF_Unwinding: Prepare some temporary files used by the fund-in-fund unwinding code
array_Prepare_MF_Unwinding="1-33"
JOB_Prepare_MF_Unwinding_ID=`sbatch \
         --partition=${partition_Prepare_MF_Unwinding} --time=${time_Prepare_MF_Unwinding} \
         --nodes=${nodes} --ntasks=${ntasks_Prepare_MF_Unwinding} --job-name=Prepare_MF_Unwinding \
          --output="${mns_erroroutput_path}/Prepare_MF_Unwinding-%A_%a.out" --error="${mns_erroroutput_path}/Prepare_MF_Unwinding-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Manual_Corrections_ID} --mem=${mem_Prepare_MF_Unwinding} --array=${array_Prepare_MF_Unwinding} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Prepare_MF_Unwinding | awk '{print $NF}'`
echo "Submitted Prepare_MF_Unwinding Job: "${JOB_Prepare_MF_Unwinding_ID}
sleep 1


# Unwind_MF_Positions_Step1: Handle the unraveling of positions of funds investing in other funds
array_Unwind_MF_Positions_Step1="1-65"
JOB_Unwind_MF_Positions_Step1_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step1} --time=${time_Unwind_MF_Positions_Step1} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step1} --job-name=Unwind_MF_Positions_Step1 \
          --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step1-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step1-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Prepare_MF_Unwinding_ID} \
         --mem=${mem_Unwind_MF_Positions_Step1} --array=${array_Unwind_MF_Positions_Step1} \
         "${mns_code_path}/build/Python_Controller.sh" ${U} Unwind_MF_Positions_Step1 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step1 Job: "${JOB_Unwind_MF_Positions_Step1_ID}
sleep 1


# Unwind_MF_Positions_Step15: Consolidate adjustment factors used to compute appropriately re-scaled versions of 
# the positions involved in the fund-in-fund unwinding
array_Unwind_MF_Positions_Step15="1-33"
JOB_Unwind_MF_Positions_Step15_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step15} --time=${time_Unwind_MF_Positions_Step15} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step15} --job-name=Unwind_MF_Positions_Step15 \
          --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step15-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step15-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step1_ID} \
         --mem=${mem_Unwind_MF_Positions_Step15} --array=${array_Unwind_MF_Positions_Step15} \
         "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step15 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step15 Job: "${JOB_Unwind_MF_Positions_Step15_ID}
sleep 1


# Unwind_MF_Positions_Step2: Re-generate holding detail data reflecting the unraveling and rescaling of fund-in-fund positions
array_Unwind_MF_Positions_Step2_Small="1-38,67-112"
JOB_Unwind_MF_Positions_Step2_Small_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step2_Small} --time=${time_Unwind_MF_Positions_Step2_Small} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step2_Small} --job-name=Unwind_MF_Positions_Step2_Small \
          --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step2_Small-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step2_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step15_ID} \
         --mem=${mem_Unwind_MF_Positions_Step2_Small} --array=${array_Unwind_MF_Positions_Step2_Small} \
         "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step2 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step2_Small Job: "${JOB_Unwind_MF_Positions_Step2_Small_ID}
sleep 1

array_Unwind_MF_Positions_Step2_Large="39-65,113-130"
JOB_Unwind_MF_Positions_Step2_Large_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step2_Large} --time=${time_Unwind_MF_Positions_Step2_Large} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step2_Large} --job-name=Unwind_MF_Positions_Step2_Large \
          --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step2_Large-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step2_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step15_ID} \
         --mem=${mem_Unwind_MF_Positions_Step2_Large} --array=${array_Unwind_MF_Positions_Step2_Large} \
         "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step2 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step2_Large Job: "${JOB_Unwind_MF_Positions_Step2_Large_ID}
sleep 1


# Unwind_MF_Positions_Step3: Re-generate holding detail data reflecting the unraveling and rescaling of fund-in-fund positions
array_Unwind_MF_Positions_Step3_Small="1-38,67-112"
JOB_Unwind_MF_Positions_Step3_Small_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step3_Small} --time=${time_Unwind_MF_Positions_Step3_Small} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step3_Small} --job-name=Unwind_MF_Positions_Step3_Small \
         --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step3_Small-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step3_Small-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step2_Small_ID}:${JOB_Unwind_MF_Positions_Step2_Large_ID} \
         --mem=${mem_Unwind_MF_Positions_Step3_Small} --array=${array_Unwind_MF_Positions_Step3_Small} \
         "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step3 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step3_Small Job: "${JOB_Unwind_MF_Positions_Step3_Small_ID}
sleep 1

array_Unwind_MF_Positions_Step3_Large="39-65,113-130"
JOB_Unwind_MF_Positions_Step3_Large_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step3_Large} --time=${time_Unwind_MF_Positions_Step3_Large} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step3_Large} --job-name=Unwind_MF_Positions_Step3_Large \
         --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step3_Large-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step3_Large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step2_Small_ID}:${JOB_Unwind_MF_Positions_Step2_Large_ID} \
         --mem=${mem_Unwind_MF_Positions_Step3_Large} --array=${array_Unwind_MF_Positions_Step3_Large} \
         "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step3 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step3_Large Job: "${JOB_Unwind_MF_Positions_Step3_Large_ID}
sleep 1


# Unwind_MF_Positions_Step4: Aggregate the fund-in-fund unraveled data to a yearly frequency
array_Unwind_MF_Positions_Step4="1-33"
JOB_Unwind_MF_Positions_Step4_ID=`sbatch \
         --partition=${partition_Unwind_MF_Positions_Step4} --time=${time_Unwind_MF_Positions_Step4} \
         --nodes=${nodes} --ntasks=${ntasks_Unwind_MF_Positions_Step4} --job-name=Unwind_MF_Positions_Step4 \
         --output="${mns_erroroutput_path}/Unwind_MF_Positions_Step4-%A_%a.out" --error="${mns_erroroutput_path}/Unwind_MF_Positions_Step4-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step3_Small_ID}:${JOB_Unwind_MF_Positions_Step3_Large_ID} \
         --mem=${mem_Unwind_MF_Positions_Step4} --array=${array_Unwind_MF_Positions_Step4} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Unwind_MF_Positions_Step4 | awk '{print $NF}'`
echo "Submitted Unwind_MF_Positions_Step4 Job: "${JOB_Unwind_MF_Positions_Step4_ID}
sleep 1


# Create_Final_Files: Generate the final "HD" (Holding Detail) files that are used in the analysis
array_Create_Final_Files="1-33"
JOB_Create_Final_Files_ID=`sbatch \
         --partition=${partition_Create_Final_Files} --time=${time_Create_Final_Files} \
         --nodes=${nodes} --ntasks=${ntasks_Create_Final_Files} --job-name=Create_Final_Files \
         --output="${mns_erroroutput_path}/Create_Final_Files-%A_%a.out" --error="${mns_erroroutput_path}/Create_Final_Files-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Unwind_MF_Positions_Step4_ID}:${JOB_Internal_Class_ID} \
         --mem=${mem_Create_Final_Files} --array=${array_Create_Final_Files} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Create_Final_Files | awk '{print $NF}'`
echo "Submitted Create_Final_Files Job: "${JOB_Create_Final_Files_ID}
sleep 1


# Industry: Compile datasets with company-level industry assignments from both Morningstar and external sources
JOB_Industry_ID=`sbatch \
         --partition=${partition_Industry} --time=${time_Industry} \
         --nodes=${nodes} --ntasks=${ntasks_Industry} --job-name=Industry \
         --output="${mns_erroroutput_path}/Industry-%A_%a.out" --error="${mns_erroroutput_path}/Industry-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         --depend=afterok:${JOB_Create_Final_Files_ID} \
         --mem=${mem_Industry} \
           "${mns_code_path}/build/Master_Build_Controller.sh" ${U} Industry | awk '{print $NF}'`
echo "Submitted Industry Job: "${JOB_Industry_ID}
sleep 1

# FINISH
echo "Finished Submitting Master_Build."
exit
