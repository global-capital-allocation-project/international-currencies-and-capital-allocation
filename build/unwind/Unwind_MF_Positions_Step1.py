# --------------------------------------------------------------------------------------------------
# Unwind_MF_Positions, Step 1
# 
# All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
# procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
# different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
#
# This jobs handles the actual unraveling of positions of funds investing in other funds. Where 
# possible, positions are attributed to the ultimate holding fund, and the positions of the investing 
# fund are scaled back accordingly.
# --------------------------------------------------------------------------------------------------
from __future__ import print_function
from itertools import chain
import distutils.dir_util
import pandas as pd
import numpy as np
import random
import os
import argparse
import logging
import resource
import gc
import getpass
import sys


# Function to compute chronological markets
def get_chron_markers(taskid, firstyear, job_frequency):

    # Split the task into quarter-sized jobs
    if job_frequency == "q":
        year = firstyear + int(np.floor(taskid / 4))
        period = taskid % 4 if (taskid % 4 != 0) else 4
        prev_year = year if period > 1 else (year - 1)
        prev_period = 4 if period == 1 else (period - 1)

    # Else split into half-year jobs
    elif job_frequency == "h":
        year = firstyear + int(np.floor(taskid / 2))
        period = int(taskid % 2 != 0) + 1
        prev_year = year if period == 2 else (year - 1)
        prev_period = 1 if period == 2 else 2

    return year, period, prev_year, prev_period


# Main routine
if __name__ == "__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--taskid", type=int, help="Year for processing (as SLURM task ID)")
    parser.add_argument("-d", "--datapath", type=str, help="MNS data path")
    parser.add_argument("-f", "--firstyear", type=int, help="First year in dataset")
    args = parser.parse_args()
    mns_data = args.datapath
    firstyear = args.firstyear
    taskid = args.taskid
    job_frequency = "h" # Hardcoded: q for quarterly; h for half-yearly
    
    # Set up logging
    logger = logging.getLogger(__name__)
    logfile = '{}/results/logs/{}_Unwind_MF_Positions_Step1_{}.log'.format(mns_data, getpass.getuser(), taskid)
    if os.path.exists(logfile):
        os.remove(logfile)
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG)
    sys.stderr = open(logfile, 'a')
    sys.stdout = open(logfile, 'a')
    logging.info("Begin Unwinding_MF_Positions")

    # Ensure relevant directories exist
    distutils.dir_util.mkpath(os.path.join(mns_data, "temp/mf_unwinding/mf_xb_reassignments"))
    distutils.dir_util.mkpath(os.path.join(mns_data, "temp/mf_unwinding/mf_scaling_lists"))
    distutils.dir_util.mkpath(os.path.join(mns_data, "temp/mf_unwinding/hd_period_info"))

    # Columns to be read in
    usecols = ['MasterPortfolioId', 'iso_currency_code', 'date', 'iso_country_code',
        'cusip', 'currency_id', 'marketvalue', 'mns_class', '_obs_id', 'lcu_per_usd_eop']

    # Get chronological markers
    year, period, prev_year, prev_period = get_chron_markers(taskid, firstyear, job_frequency)

    # Report period
    logger.warning("Running MF positions unwinding for year {}, {} = {}".format(year, job_frequency, period))

    # Read in link table
    logger.warning("Reading in the link table")
    links = pd.read_stata(os.path.join(mns_data, 
        "output/morningstar_api_data/Morningstar_API_Data_Link_Table.dta"))
    links['_count'] = 1
    link_counts = pd.DataFrame(links.groupby('cusip')['_count'].sum())
    links = links[~links.cusip.isin(set(link_counts[link_counts['_count'] > 1].index))]
    del links['_count']
    del link_counts

    # Load in the files
    current_period_nonus = pd.read_stata(
        os.path.join(mns_data, "temp/mf_unwinding/tmp_hd_files/NonUS_{}_{}{}_m_step4.dta".format(year, job_frequency, period)), columns=usecols)
    current_period_us = pd.read_stata(
        os.path.join(mns_data, "temp/mf_unwinding/tmp_hd_files/US_{}_{}{}_m_step4.dta".format(year, job_frequency, period)), columns=usecols)
    prev_period_nonus = pd.read_stata(
        os.path.join(mns_data, "temp/mf_unwinding/tmp_hd_files/NonUS_{}_{}{}_m_step4.dta".format(prev_year, job_frequency, prev_period)), columns=usecols)
    prev_period_us = pd.read_stata(
        os.path.join(mns_data, "temp/mf_unwinding/tmp_hd_files/US_{}_{}{}_m_step4.dta".format(prev_year, job_frequency, prev_period)), columns=usecols)

    # Take special care of the first period (I verified there are no positions to be unwound)
    if taskid == 1:
        logger.warning("Processing primer period")
        prev_period_nonus['mf_unwound'] = 0
        prev_period_us['mf_unwound'] = 0
        outcols = ["MasterPortfolioId", "_obs_id", "cusip", "date", "marketvalue", "mf_unwound"]
        prev_period_nonus[outcols].to_stata(os.path.join(mns_data, "temp/mf_unwinding/hd_period_info/NonUS_{}_{}{}_m_step51.dta".format(year, job_frequency, 1)), write_index=False)
        prev_period_us[outcols].to_stata(os.path.join(mns_data, "temp/mf_unwinding/hd_period_info/US_{}_{}{}_m_step51.dta".format(year, job_frequency, 1)), write_index=False)
        del prev_period_nonus['mf_unwound']
        del prev_period_us['mf_unwound']

    logger.warning("Checkpoint 1")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Concatenate
    current_period_nonus['nonus'] = 1
    current_period_us['nonus'] = 0
    prev_period_nonus['nonus'] = 1
    prev_period_us['nonus'] = 0
    current_period = pd.concat([current_period_nonus, current_period_us], axis=0, sort=False)
    prev_period = pd.concat([prev_period_nonus, prev_period_us], axis=0, sort=False)
    current_period['current'] = 1
    prev_period['current'] = 0
    current_period_cols = set(current_period.columns)

    # Perform currency conversion
    current_period['marketvalue_usd'] = current_period['marketvalue'] / current_period['lcu_per_usd_eop']
    prev_period['marketvalue_usd'] = prev_period['marketvalue'] / prev_period['lcu_per_usd_eop']

    logger.warning("Checkpoint 2")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Clearing memory
    del [[current_period_nonus, current_period_us, prev_period_us, prev_period_nonus]]
    gc.collect()
    current_period_nonus, current_period_us = pd.DataFrame(), pd.DataFrame()
    prev_period_us, prev_period_nonus = pd.DataFrame(), pd.DataFrame()
    
    logger.warning("Checkpoint 3")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Match to the link table
    logger.warning("Preparing for unwinding")
    current_period_mf_positions = current_period[current_period.cusip.isin(set(links.cusip))].merge(
        links[['cusip', 'investing_mpid']], on=["cusip"], how="left").dropna(subset=['investing_mpid'])

    # Unwind the positions: preparation
    full_positions = pd.concat([current_period, prev_period], axis=0, sort=False).sort_values('date')
    del [[current_period, prev_period]]
    gc.collect()
    current_period, prev_period = pd.DataFrame(), pd.DataFrame()
    current_period_mf_positions.investing_mpid = current_period_mf_positions.investing_mpid.astype(int)
    full_positions['investing_mpid'] = full_positions['MasterPortfolioId']
    full_positions.investing_mpid = full_positions.investing_mpid.fillna(-1).astype(int)
    current_period_mf_positions = current_period_mf_positions.sort_values('date')
    full_positions['date_investing'] = full_positions['date']

    logger.warning("Checkpoint 4")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # First asof merge to link to reporting dates
    logger.warning("Unwinding: Merge 1")
    current_period_mf_positions['date'] = pd.to_datetime(current_period_mf_positions['date'])
    unwind_step1_links = pd.merge_asof(
        current_period_mf_positions[
            ['cusip', 'MasterPortfolioId', 'investing_mpid', 'date', 'marketvalue', 
             'marketvalue_usd', 'currency_id', 'iso_country_code', '_obs_id', 'nonus']
        ].sort_values('date').rename(
            columns={'currency_id': 'holding_currency_id', 
                     'iso_country_code': 'holding_iso_country_code', 
                     '_obs_id': 'holding_obs_id',
                     'nonus': 'holding_nonus'}
        ),
        full_positions[['investing_mpid', 'date', 'date_investing']].sort_values('date').dropna(subset=["date"]).drop_duplicates(), 
        on=['date'], by=["investing_mpid"], 
        tolerance=pd.Timedelta(6, "M")).dropna(subset=["date_investing"])

    logger.warning("Checkpoint 5")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Second merge to reconstruct the positions
    logger.warning("Unwinding: Merge 2")
    unwind_step2_positions = unwind_step1_links.merge(full_positions.drop(['date'], axis=1), 
        how="left", on=['investing_mpid', 'date_investing'], 
        suffixes=("", "_investing")).dropna(subset=['marketvalue_investing'])
    del unwind_step1_links; gc.collect()
    unwind_step1_links = pd.DataFrame()

    logger.warning("Checkpoint 6")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Third merge to find funds' NAV
    logger.warning("Unwinding: Merge 3")
    full_positions['marketvalue_usd_abs'] = np.abs(full_positions['marketvalue_usd'])
    total_navs = pd.DataFrame(full_positions[full_positions.investing_mpid > -1].groupby(['investing_mpid', 'date'])['marketvalue_usd_abs'].sum()).reset_index()
    full_positions = full_positions.drop(['marketvalue_usd_abs'], axis=1)
    total_navs = total_navs.rename(columns={'marketvalue_usd_abs': 'fund_nav_usd', 'date': 'date_investing'})
    unwind_step3_positions = unwind_step2_positions.merge(total_navs, on=['investing_mpid', 'date_investing'], how='left')
    del unwind_step2_positions; gc.collect()
    unwind_step2_positions = pd.DataFrame()

    logger.warning("Checkpoint 7")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Infer the size of the positions
    # Note that there are instances in the data in which the reported position of the holding fund is greater than the total market value 
    # of all the positions held by the investing fund. In these cases I allow NAV% to be >1.
    unwind_step3_positions['holding_nav_percent'] = unwind_step3_positions['marketvalue_usd'].abs() / unwind_step3_positions['fund_nav_usd']
    unwind_step3_positions['inferred_position_usd'] = (unwind_step3_positions['marketvalue_usd_investing'] * unwind_step3_positions['holding_nav_percent'])
    unwind_step3_positions['inferred_position'] = (unwind_step3_positions['inferred_position_usd'] * unwind_step3_positions['lcu_per_usd_eop'])

    # Save ancillary reports
    unwind_step3_positions.to_pickle(
        os.path.join(mns_data, "temp/mf_unwinding/mf_xb_reassignments", "mf_xbr_{}_{}{}.pkl".format(year, job_frequency, period))
    )
    logger.warning("Saving file {}".format(os.path.join(mns_data, "temp/mf_unwinding/mf_xb_reassignments", "mf_xbr_{}_{}{}.pkl".format(year, job_frequency, period))))

    # Positions to be deleted from dataset (unwound)
    logger.warning("Unwinding: Merge 4")
    unwound_idx = unwind_step3_positions['holding_obs_id'].values

    # Standardize fields
    full_positions['date'] = pd.to_datetime(full_positions['date'])
    full_positions['cusip'] = full_positions['cusip'].astype(str)
    full_positions['_obs_id'] = full_positions['_obs_id'].astype(str)
    full_positions['MasterPortfolioId'] = full_positions['MasterPortfolioId'].astype(int)
    full_positions['marketvalue'] = full_positions['marketvalue'].astype(float)
    full_positions['marketvalue_usd'] = full_positions['marketvalue_usd'].astype(float)

    # Also store the unmatched position stats
    logger.warning("Storing not-unwound position stats")    
    full_positions[(full_positions.current == 1) & ~(full_positions._obs_id.isin(unwound_idx)) & (full_positions.cusip.isin(set(links.cusip)))][
            ['MasterPortfolioId', 'cusip', 'marketvalue', 'marketvalue_usd', '_obs_id', 'date', 'current', 'nonus']
    ].to_stata(
        os.path.join(mns_data, "temp/mf_unwinding/mf_xb_reassignments", "mf_xbr_{}_{}{}_not_unwound.dta".format(year, job_frequency, period)), write_index=False
    )
    full_positions[(full_positions.current == 1) & ~(full_positions.cusip.isin(set(links.cusip)))][
            ['MasterPortfolioId', 'cusip', 'marketvalue', 'marketvalue_usd', '_obs_id', 'date', 'current', 'nonus']
    ].to_stata(
        os.path.join(mns_data, "temp/mf_unwinding/mf_xb_reassignments", "mf_xbr_{}_{}{}_non_mf.dta".format(year, job_frequency, period)), write_index=False
    )

    # Prepare new positions for inclusion in the dataset
    unwind_step4_positions = unwind_step3_positions.copy()
    unwind_step4_positions['cusip'] = unwind_step4_positions['cusip_investing']
    unwind_step4_positions['marketvalue'] = unwind_step4_positions['inferred_position']
    unwind_step4_positions = unwind_step4_positions[['MasterPortfolioId', 'cusip', 'marketvalue', '_obs_id', 'date', 'current', 'holding_nonus']]
    unwind_step4_positions['mf_unwound'] = 1

    logger.warning("Checkpoint 8")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Rescale MF positions
    # As noted above, sometimes the reported position of the holding fund is greater than the total market value 
    # of all the positions held by the investing fund. In these cases I impose a floor of 0 on the rescaling factor.
    logger.warning("Unwinding: Rescaling")
    mf_rescaling_list = pd.DataFrame(unwind_step3_positions.groupby('_obs_id')['holding_nav_percent'].sum()).reset_index()
    mf_rescaling_list['mf_scaling_factor'] = np.maximum(1 - mf_rescaling_list['holding_nav_percent'], 0.)
    mf_rescaling_list['mf_rescaled'] = 1
    mf_rescaling_list = mf_rescaling_list.drop('holding_nav_percent', axis=1)

    logger.warning("Checkpoint 9")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Store rescaling list
    mf_rescaling_list.to_pickle(os.path.join(mns_data, "temp/mf_unwinding/mf_scaling_lists", "mf_scalings_{}_{}{}.pkl".format(year, job_frequency, period)))
    if mf_rescaling_list.shape[0] == 0:
        mf_rescaling_list = mf_rescaling_list.reindex(mf_rescaling_list.index.values.tolist()+['1'])
        mf_rescaling_list['mf_rescaled'] = mf_rescaling_list['mf_rescaled'].astype(bool)
        mf_rescaling_list['_obs_id'] = ""
        mf_rescaling_list['mf_scaling_factor'] = mf_rescaling_list['mf_rescaled'].astype(float)
        logger.warning(mf_rescaling_list.to_string())
    mf_rescaling_list.to_stata(os.path.join(mns_data, "temp/mf_unwinding/mf_scaling_lists", "mf_scalings_{}_{}{}.dta".format(year, job_frequency, period)), write_index=False)

    # Finalize the outfile
    logger.warning("Unwinding: Finalizing outfiles")
    current_period_out = pd.concat([
        full_positions[(full_positions.current == 1) & ~(full_positions._obs_id.isin(unwound_idx))][
            ['MasterPortfolioId', 'cusip', 'marketvalue', '_obs_id', 'date', 'current', 'nonus']
        ],
        unwind_step4_positions.rename(columns={'holding_nonus': 'nonus'})
    ], sort=False).drop(['current'], axis=1).sort_values('date', ascending=True)
    current_period_out['mf_unwound'] = current_period_out['mf_unwound'].fillna(0).astype(bool)
    initial_col_set = set(current_period_out.columns)

    # Convert fields
    current_period_out['date'] = pd.to_datetime(current_period_out['date'])
    current_period_out['cusip'] = current_period_out['cusip'].astype(str)
    current_period_out['_obs_id'] = current_period_out['_obs_id'].astype(str)
    current_period_out['MasterPortfolioId'] = current_period_out['MasterPortfolioId'].astype(int)
    current_period_out['marketvalue'] = current_period_out['marketvalue'].astype(float)

    # Store the outfile
    current_period_out[current_period_out.nonus == 1].drop(['nonus'], axis=1).to_stata(
        os.path.join(mns_data, "temp/mf_unwinding/hd_period_info/NonUS_{}_{}{}_m_step51.dta".format(year, job_frequency, period)), write_index=False)
    current_period_out[current_period_out.nonus == 0].drop(['nonus'], axis=1).to_stata(
        os.path.join(mns_data, "temp/mf_unwinding/hd_period_info/US_{}_{}{}_m_step51.dta".format(year, job_frequency, period)), write_index=False)

    logger.warning("Checkpoint 10")
    logger.warning('Memory usage: %d (MB)' % (int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024))

    # Close logs
    sys.stderr.close()
    sys.stdout.close()
    sys.stderr = sys.__stderr__
    sys.stdout = sys.__stdout__
