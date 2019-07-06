# --------------------------------------------------------------------------------------------------
# Ultimate parent aggregation
#
# This file provides an implementation of the ultimate parent aggregation algorithm of Coppola,
# Maggiori, Neiman, and Schreger (2019). For a detailed discussion of the aggregation algorithm, please
# refer to that paper. The algorithm aggregates ultimate parent (UP) and domicile information coming
# from CUSIP Global Services (CGS), Morningstar, Capital IQ (CIQ), SDC Platinum, Orbis, and Factset.
# The algorithm ultimately associates the universe of traded equity and debt securities with their 
# issuer’s ultimate parent.
#
# Key output: temp/country_master/up_aggregation_final_compact.dta
#
# Key variables:
#       issuer_number:          The cusip6 of each issuer
#       issuer_name:            The name of the issuer in issuer_number
#       cgs_domicile:           The country of domicile of the issuer in issuer_number 
#                                   as in the CGS masterfile 
#       cusip6_bg:              The best guess of cusip6 of the ultimate parent/associated 
#                                   issuer for the issuer in issuer_number
#       issuer_name_up:         The name of the issuer in cusip6_bg
#       country_bg:             The best guess of the country of domicile of the issuer 
#                                   in the cusip6_bg
#       country_bg_source:      The source data used for the guess in country_bg
#       cusip6_up_bg_source:    The source data used for the guess in cusip6_up_bg
# --------------------------------------------------------------------------------------------------
import itertools
import pandas as pd
import numpy as np
import os
import argparse
import logging
import resource
import gc
import getpass
import sys
from statistics import mode
from tqdm import tqdm, tqdm_pandas
from UP_Helper import flatten_parent_child_map, detect_cycle, get_circular_elements
from UP_Helper import resolve_top_source_country_conflict, get_unique_iso_with_sources
from UP_Helper import get_multiple_modes, resolve_cross_ownership_chains
tqdm.pandas()

from Project_Constants import tax_havens, source_preference_order


# Main decision function for UP aggregation
def determine_ultimate_parent_and_country_assignment(row, source_preference_order, country_details_master):
    """
    This function performs UP and country aggregation via a series of decision rules.
    
    Parameters:
        row: The input dataframe row
        source_preference_order: Arbitrary source preference ordering (dictionary)
        country_details_master: A dictionary listing each source's country assignment for a given CUSIP6

    Returns: 
        (str: up_cusip6_bg, str: country_bg, int: case_code, str: up_cusip6_bg_final_source,
            str: country_bg_final_source, str: extra_info).
    """

    # Some common objects
    ciq_sdc_bvd_dlg_fds_up_cusip6 = [row.cusip6_up_ciq, row.cusip6_up_sdc, row.cusip6_up_bvd, row.cusip6_up_dlg, row.cusip6_up_fds]
    ciq_sdc_bvd_dlg_fds_countries = [row.country_ciq, row.country_sdc, row.country_bvd, row.country_dlg, row.country_fds]
    ciq_sdc_bvd_dlg_fds_sources = ["ciq", "sdc", "bvd", "dlg", "fds"]
    
    # CIQ, SDC, BVD, DLG, FDS all present and agree
    if (row.cusip6_up_ciq != "" and row.cusip6_up_sdc != "" and row.cusip6_up_bvd != "" and 
        row.cusip6_up_dlg != "" and row.cusip6_up_fds != "" and
        row.cusip6_up_ciq == row.cusip6_up_sdc and row.cusip6_up_ciq == row.cusip6_up_bvd and
        row.cusip6_up_sdc == row.cusip6_up_bvd and row.cusip6_up_sdc == row.cusip6_up_dlg and
        row.cusip6_up_dlg == row.cusip6_up_fds):
        
        # Agree on non-FP
        if (row.country_ciq not in tax_havens and row.country_ciq != "" and get_country_fallback_for_cusip(row.cusip6_up_ciq,
            source_preference_order, country_details_master)[0] not in tax_havens):
            return_tuple = (row.cusip6_up_ciq, row.country_ciq, 1, "ciq sdc bvd dlg fds", "ciq sdc bvd dlg fds", "")

        # Agree on FP; AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 2, "cusip6_up_ai", "country_ai", "")

        # Agree on FP; AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 3, "cusip6_up_ai", "country_ms_ai", "")
        
        # Agree on FP; CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 4, "immediate_issuer_number", "cgs_domicile", "")
    
        # Agree on FP; CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 5, "immediate_issuer_number", "country_ms", "")

        # Agree on FP; AI and CGS also FP
        else:
            return_tuple = (row.cusip6_up_ciq, row.country_ciq, 6, "ciq sdc bvd dlg fds", "ciq sdc bvd dlg fds", "")

    # Only one of CIQ, SDC, BVD, DLG, FDS is present
    elif sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) == 1:
        
        # Find out which one is non-blank
        ix = np.argmax([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6])
        nonblank_issuer_number = ciq_sdc_bvd_dlg_fds_up_cusip6[ix]
        nonblank_country = ciq_sdc_bvd_dlg_fds_countries[ix]
        nonblank_source = ciq_sdc_bvd_dlg_fds_sources[ix]

        # Code is non-FP
        if (nonblank_country not in tax_havens and get_country_fallback_for_cusip(nonblank_issuer_number, 
            source_preference_order, country_details_master)[0] not in tax_havens):
            return_tuple = (nonblank_issuer_number, nonblank_country, 7, nonblank_source, nonblank_source, "")

        # AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 8, "cusip6_up_ai", "country_ai", "")

        # AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 9, "cusip6_up_ai", "country_ms_ai", "")
        
        # CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 10, "immediate_issuer_number", "cgs_domicile", "")
    
        # CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 11, "immediate_issuer_number", "country_ms", "")

        # Everything is FP; use non-blank code
        else:
            return_tuple = (nonblank_issuer_number, nonblank_country, 12, nonblank_source, nonblank_source, "")

    # None of CIQ, SDC, BVD, DLG, FDS are present
    elif sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) == 0:
        
        # AI is non-FP
        if row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 13, "cusip6_up_ai", "country_ai", "")

        # AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 14, "cusip6_up_ai", "country_ms_ai", "")
        
        # CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 15, "immediate_issuer_number", "cgs_domicile", "")
    
        # CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 16, "immediate_issuer_number", "country_ms", "")

        # Simply return AI issuer number and domicile, though FP
        elif row.cusip6_up_ai != "":
            return_tuple = (row.cusip6_up_ai, row.country_ai, 17, "cusip6_up_ai", "country_ai", "")
        
        # Return security issuer number and CGS domicile, though FP
        else:
            return_tuple = (row.issuer_number, row.cgs_domicile, 18, "immediate_issuer_number", "cgs_domicile", "")

    # 2 to 5 of CIQ, SDC, BVD, DLG, FDS are present and all disagree
    elif (sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) >= 2 and sum([x == y for (x, y) in 
            itertools.combinations([x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""], 2)]) == 0):
        
        # Find out what the sources are
        nonblank_codes_list = [x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""]
        nonblank_codes = {}
        sources = {}
        corresponding_countries = {}
        nfp_indicator = {}
        for (i, code) in enumerate(nonblank_codes_list):
            nonblank_codes[i] = code
            if code == row.cusip6_up_ciq:
                sources[i] = "ciq"
                corresponding_countries[i] = ciq_sdc_bvd_dlg_fds_countries[0]
            elif code == row.cusip6_up_sdc:
                sources[i] = "sdc"
                corresponding_countries[i] = ciq_sdc_bvd_dlg_fds_countries[1]
            elif code == row.cusip6_up_dlg:
                sources[i] = "dlg"
                corresponding_countries[i] = ciq_sdc_bvd_dlg_fds_countries[3]
            elif code == row.cusip6_up_fds:
                sources[i] = "fds"
                corresponding_countries[i] = ciq_sdc_bvd_dlg_fds_countries[4]
            else:
                sources[i] = "bvd"
                corresponding_countries[i] = ciq_sdc_bvd_dlg_fds_countries[2]
            if corresponding_countries[i] not in tax_havens:
                nfp_indicator[i] = 1
            else:
                nfp_indicator[i] = 0

        # How many are non-FP?
        nfp_count = sum([(x not in tax_havens and x != "" and get_country_fallback_for_cusip(y,
                source_preference_order, country_details_master)[0] not in tax_havens) 
            for (x,y) in zip(ciq_sdc_bvd_dlg_fds_countries, ciq_sdc_bvd_dlg_fds_up_cusip6)])
        if len([x for x in corresponding_countries.values() if x not in tax_havens]) == 0:
            nfp_count = 0

        # One is non-FP; use it
        if nfp_count == 1:
            
            # Find out which one is non-FP    
            nfp_country = [x for x in corresponding_countries.values() if x not in tax_havens][0]
            nfp_key = [x for x in corresponding_countries.keys() if corresponding_countries[x] == nfp_country][0]
            nfp_source = sources[nfp_key]
            nfp_code = nonblank_codes[nfp_key]
            
            return_tuple = (nfp_code, nfp_country, 19, nfp_source, nfp_source, "")
    
        # All are FP
        if nfp_count == 0:
    
            # AI is non-FP
            if row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
                return_tuple = (row.cusip6_up_ai, row.country_ai, 20, "cusip6_up_ai", "country_ai", "")

            # AI is FP but MS modal report for AI is non-FP
            elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
                return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 21, "cusip6_up_ai", "country_ms_ai", "")
            
            # CGS domicile is non-FP
            elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
                return_tuple = (row.issuer_number, row.cgs_domicile, 22, "immediate_issuer_number", "cgs_domicile", "")
        
            # CGS domicile is FP but MS modal report is non-FP
            elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
                return_tuple = (row.issuer_number, row.country_ms, 23, "immediate_issuer_number", "country_ms", "")

            # Else use whichever source is preferred
            else:
                if source_preference_order[sources[0]] < source_preference_order[sources[1]]:
                    pref_source = sources[0]
                    pref_index = 0
                else:
                    pref_source = sources[1]
                    pref_index = 1
                
                return_tuple = (nonblank_codes[pref_index], corresponding_countries[pref_index], 24, pref_source, pref_source, "")
        
        # More than one are non-FP
        if nfp_count > 1:
            
            # Use whichever is preferred
            current_min_index = 100
            for i in range(len(nonblank_codes_list)):
                if nfp_indicator[i] == 1 and source_preference_order[sources[i]] < current_min_index:
                    current_min_index = source_preference_order[sources[i]]
                    pref_source = sources[i]
                    pref_index = i
                    
            return_tuple = (nonblank_codes[pref_index], corresponding_countries[pref_index], 25, pref_source, pref_source, "")

    # Between 3 and 5 are present, and three or four of them agree
    elif (sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) >= 3 and 
        sum([x == y == z for (x, y, z) in itertools.combinations(ciq_sdc_bvd_dlg_fds_up_cusip6, 3)]) >= 1):

        # Find out which ones agree
        agreed_issuer_num = mode([x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""])
        outlier_issuer_nums = [x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != agreed_issuer_num]

        # Find agreement sources
        agreement_sources_ix = np.argwhere(np.array(ciq_sdc_bvd_dlg_fds_up_cusip6) == agreed_issuer_num)
        agreement_sources = [ciq_sdc_bvd_dlg_fds_sources[i] for i in [int(x) for x in agreement_sources_ix]]
        outlier_sources = [ciq_sdc_bvd_dlg_fds_sources[i] for i in range(5) if i not in [int(x) for x in agreement_sources_ix]]
        agreed_country = row['country_{}'.format(agreement_sources[0])]
        outlier_countries = [row['country_{}'.format(x)] for x in outlier_sources]

        # Agreement code is not FP; use it
        if (agreed_country not in tax_havens and get_country_fallback_for_cusip(agreed_issuer_num, 
            source_preference_order, country_details_master)[0] not in tax_havens):
            return_tuple = (agreed_issuer_num, agreed_country, 26, " ".join(agreement_sources), " ".join(agreement_sources), "")
        
        # Agreement code is FP; one outlier code is not
        elif sum([outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens 
                for (outlier_issuer_num, outlier_country) in zip(outlier_issuer_nums, outlier_countries)]) == 1:

            outlier_issuer_num, outlier_country, outlier_source = [
                (outlier_issuer_num, outlier_country, outlier_source) for
                (outlier_issuer_num, outlier_country, outlier_source) in zip(outlier_issuer_nums, outlier_countries, outlier_sources)
                if outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens][0]
            return_tuple = (outlier_issuer_num, outlier_country, 27, outlier_source, outlier_source, "")

        # Agreement code is FP; two outlier codes are not
        elif sum([outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens 
                for (outlier_issuer_num, outlier_country) in zip(outlier_issuer_nums, outlier_countries)]) == 2:

            outliers = [ (outlier_issuer_num, outlier_country, outlier_source) for
                (outlier_issuer_num, outlier_country, outlier_source) in zip(outlier_issuer_nums, outlier_countries, outlier_sources)
                if outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens]

            outlier_issuer_num_x, outlier_country_x, outlier_source_x = outliers[0]
            outlier_issuer_num_y, outlier_country_y, outlier_source_y = outliers[1]
            
            if source_preference_order[outlier_source_x] < source_preference_order[outlier_source_y]:
                return_tuple = (outlier_issuer_num_x, outlier_country_x, 28, outlier_source_x, outlier_source_x, "")
            else:
                return_tuple = (outlier_issuer_num_y, outlier_country_y, 29, outlier_source_y, outlier_source_y, "")

        # AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 30, "cusip6_up_ai", "country_ai", "")

        # AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 31, "cusip6_up_ai", "country_ms_ai", "")
        
        # CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 32, "immediate_issuer_number", "cgs_domicile", "")
    
        # CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 33, "immediate_issuer_number", "country_ms", "")

        # Everything is FP; use agreement code
        else:
            return_tuple = (agreed_issuer_num, agreed_country, 34, " ".join(agreement_sources), " ".join(agreement_sources), "")

    # Between 2 and 5 are present, and only two of them agree
    elif (sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) >= 2 and 
        sum([x == y for (x, y) in itertools.combinations([x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""], 2)]) == 1):

        # Find out which ones agree
        agreed_issuer_num = mode([x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""])
        outlier_issuer_nums = [x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != agreed_issuer_num]

        # Find agreement sources
        agreement_sources_ix = np.argwhere(np.array(ciq_sdc_bvd_dlg_fds_up_cusip6) == agreed_issuer_num)
        agreement_sources = [ciq_sdc_bvd_dlg_fds_sources[i] for i in [int(x) for x in agreement_sources_ix]]
        outlier_sources = [ciq_sdc_bvd_dlg_fds_sources[i] for i in range(5) if i not in [int(x) for x in agreement_sources_ix]]
        agreed_country = row['country_{}'.format(agreement_sources[0])]
        outlier_countries = [row['country_{}'.format(x)] for x in outlier_sources]

        # Agreement code is not FP; use it
        if (agreed_country not in tax_havens and get_country_fallback_for_cusip(agreed_issuer_num, 
            source_preference_order, country_details_master)[0] not in tax_havens):
            return_tuple = (agreed_issuer_num, agreed_country, 35, " ".join(agreement_sources), " ".join(agreement_sources), "")
        
        # Agreement code is FP; one outlier code is not
        elif sum([outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens 
                for (outlier_issuer_num, outlier_country) in zip(outlier_issuer_nums, outlier_countries)]) == 1:

            outlier_issuer_num, outlier_country, outlier_source = [
                (outlier_issuer_num, outlier_country, outlier_source) for
                (outlier_issuer_num, outlier_country, outlier_source) in zip(outlier_issuer_nums, outlier_countries, outlier_sources)
                if outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens][0]
            return_tuple = (outlier_issuer_num, outlier_country, 36, outlier_source, outlier_source, "")

        # Agreement code is FP; two outlier codes are not
        elif sum([outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens 
                for (outlier_issuer_num, outlier_country) in zip(outlier_issuer_nums, outlier_countries)]) == 2:

            outliers = [ (outlier_issuer_num, outlier_country, outlier_source) for
                (outlier_issuer_num, outlier_country, outlier_source) in zip(outlier_issuer_nums, outlier_countries, outlier_sources)
                if outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens]

            outlier_issuer_num_x, outlier_country_x, outlier_source_x = outliers[0]
            outlier_issuer_num_y, outlier_country_y, outlier_source_y = outliers[1]
            
            if source_preference_order[outlier_source_x] < source_preference_order[outlier_source_y]:
                return_tuple = (outlier_issuer_num_x, outlier_country_x, 37, outlier_source_x, outlier_source_x, "")
            else:
                return_tuple = (outlier_issuer_num_y, outlier_country_y, 38, outlier_source_y, outlier_source_y, "")

        # Agreement code is FP; three outlier codes are not
        elif sum([outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens 
                for (outlier_issuer_num, outlier_country) in zip(outlier_issuer_nums, outlier_countries)]) == 3:

            outliers = [ (outlier_issuer_num, outlier_country, outlier_source) for
                (outlier_issuer_num, outlier_country, outlier_source) in zip(outlier_issuer_nums, outlier_countries, outlier_sources)
                if outlier_issuer_num != "" and outlier_country != "" and outlier_country not in tax_havens]

            outlier_issuer_num_x, outlier_country_x, outlier_source_x = outliers[0]
            outlier_issuer_num_y, outlier_country_y, outlier_source_y = outliers[1]
            outlier_issuer_num_z, outlier_country_z, outlier_source_z = outliers[2]
            
            min_pref_order = min([
                source_preference_order[outlier_source_x], source_preference_order[outlier_source_y], source_preference_order[outlier_source_z]
            ])

            if source_preference_order[outlier_source_x] == min_pref_order:
                return_tuple = (outlier_issuer_num_x, outlier_country_x, 39, outlier_source_x, outlier_source_x, "")
            elif source_preference_order[outlier_source_y] == min_pref_order:
                return_tuple = (outlier_issuer_num_y, outlier_country_y, 40, outlier_source_y, outlier_source_y, "")
            else:
                return_tuple = (outlier_issuer_num_z, outlier_country_z, 41, outlier_source_z, outlier_source_z, "")

        # AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 42, "cusip6_up_ai", "country_ai", "")

        # AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 43, "cusip6_up_ai", "country_ms_ai", "")
        
        # CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 44, "immediate_issuer_number", "cgs_domicile", "")
    
        # CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 45, "immediate_issuer_number", "country_ms", "")

        # Everything is FP; use agreement code
        else:
            return_tuple = (agreed_issuer_num, agreed_country, 46, " ".join(agreement_sources), " ".join(agreement_sources), "")
 
    # Four or five are present and there are two majorities
    elif (sum([x != "" for x in ciq_sdc_bvd_dlg_fds_up_cusip6]) >= 4 and 
        sum([x == y for (x, y) in itertools.combinations([z for z in ciq_sdc_bvd_dlg_fds_up_cusip6 if z != ""], 2)]) == 2):

        # Find the two majorities
        agreed_issuer_nums = list(get_multiple_modes([x for x in ciq_sdc_bvd_dlg_fds_up_cusip6 if x != ""]))
        assert len(agreed_issuer_nums) == 2

        # Find agreement sources
        agreement_sources_x_ix = np.argwhere(np.array(ciq_sdc_bvd_dlg_fds_up_cusip6) == agreed_issuer_nums[0])
        agreement_sources_y_ix = np.argwhere(np.array(ciq_sdc_bvd_dlg_fds_up_cusip6) == agreed_issuer_nums[1])
        agreement_sources_x = [ciq_sdc_bvd_dlg_fds_sources[i] for i in [int(x) for x in agreement_sources_x_ix]]
        agreement_sources_y = [ciq_sdc_bvd_dlg_fds_sources[i] for i in [int(x) for x in agreement_sources_y_ix]]

        agreed_country_x = row['country_{}'.format(agreement_sources_x[0])]
        agreed_country_y = row['country_{}'.format(agreement_sources_y[0])]
        prefs_x = [source_preference_order[i] for i in agreement_sources_x]
        prefs_y = [source_preference_order[i] for i in agreement_sources_y]

        # Find outlier
        outlier_source = [source for source in ciq_sdc_bvd_dlg_fds_sources if source not in agreement_sources_x + agreement_sources_y]
        assert len(outlier_source) == 1
        outlier_source = outlier_source[0]
        outlier_country = row['country_{}'.format(outlier_source)]
        outlier_cusip6 = row['cusip6_up_{}'.format(outlier_source)]

        # How many are non-FP?
        nfp_count = sum([(x not in tax_havens and x != "" and get_country_fallback_for_cusip(y,
                source_preference_order, country_details_master)[0] not in tax_havens) 
            for (x,y) in zip([agreed_country_x, agreed_country_y], agreed_issuer_nums)])

        # One is non-FP
        if nfp_count == 1:
            if agreed_country_x != "" and agreed_country_x not in tax_havens:
                return_tuple = (agreed_issuer_nums[0], agreed_country_x, 47, " ".join(agreement_sources_x), " ".join(agreement_sources_x), "")
            elif agreed_country_y != "" and agreed_country_y not in tax_havens:
                return_tuple = (agreed_issuer_nums[1], agreed_country_y, 48, " ".join(agreement_sources_y), " ".join(agreement_sources_y), "")
            else:
                raise Exception("Something went wrong in UP aggregation; stack checkpoint 2")

        # Two are non-FP
        elif nfp_count == 2:

            # Find out which has the most preferred source
            if min(prefs_x) == 1:
                return_tuple = (agreed_issuer_nums[0], agreed_country_x, 49, " ".join(agreement_sources_x), " ".join(agreement_sources_x), "")
            else:
                return_tuple = (agreed_issuer_nums[1], agreed_country_y, 50, " ".join(agreement_sources_y), " ".join(agreement_sources_y), "")

        # Outlier is non-FP
        elif outlier_country != "" and outlier_cusip6 != "" and outlier_country not in tax_havens:
            return_tuple = (outlier_cusip6, outlier_country, 51, outlier_source, outlier_source, "")

        # AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ai, 52, "cusip6_up_ai", "country_ai", "")

        # AI is FP but MS modal report for AI is non-FP
        elif row.cusip6_up_ai != "" and row.country_ai != "" and row.country_ai in tax_havens and row.country_ms_ai != "" and row.country_ms_ai not in tax_havens:
            return_tuple = (row.cusip6_up_ai, row.country_ms_ai, 53, "cusip6_up_ai", "country_ms_ai", "")
        
        # CGS domicile is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile not in tax_havens:
            return_tuple = (row.issuer_number, row.cgs_domicile, 54, "immediate_issuer_number", "cgs_domicile", "")
    
        # CGS domicile is FP but MS modal report is non-FP
        elif row.cgs_domicile != "" and row.cgs_domicile in tax_havens and row.country_ms != "" and row.country_ms not in tax_havens:
            return_tuple = (row.issuer_number, row.country_ms, 55, "immediate_issuer_number", "country_ms", "")

        # Everything is FP; use best source
        else:
            if min(prefs_x) == 1:
                return_tuple = (agreed_issuer_nums[0], agreed_country_x, 56, " ".join(agreement_sources_x), " ".join(agreement_sources_x), "")
            else:
                return_tuple = (agreed_issuer_nums[1], agreed_country_y, 57, " ".join(agreement_sources_y), " ".join(agreement_sources_y), "")

    # Everything else (there should be none of these)
    else:
        return_tuple = ("", "", -1, "", "", "")

    # Final correction for  which the resolved CUSIP is associated to a blank country code
    if return_tuple[1] == "" and return_tuple[2] > -1:

        issuer_num = return_tuple[0] 
        source_preference_order_inv = {v:k for k,v in source_preference_order.items()}
        country_fallback, fallback_code = get_country_fallback_for_cusip(issuer_num, 
                source_preference_order, country_details_master)
        extra_messages = {
            'A': "blank country replaced with {}".format(source_preference_order_inv[1]),
            'B': "blank country replaced with {}".format(source_preference_order_inv[2]),
            'C': "blank country replaced with {}".format(source_preference_order_inv[3]),
            'D': "blank country replaced with {}".format(source_preference_order_inv[4]),
            'E': "blank country replaced with {}".format(source_preference_order_inv[5]),
            'F': "blank country replaced with country_ai",
            'G': "blank country replaced with country_ms_ai",
            'H': "blank country replaced with cgs_domicile",
            'I': "blank country replaced with country_ms",
            'J': "blank country replaced with cgs_domicile",
            'K': "no non-blank country code present"
        }

        return_tuple = (
            return_tuple[0], 
            country_fallback,
            return_tuple[2] + 100,
            return_tuple[3],
            return_tuple[4],
            extra_messages[fallback_code]
        )

    return return_tuple


# A helper function to inspect the best country guess for a CUSIP associated with a blank country code
def get_country_fallback_for_cusip(issuer_num_up, source_preference_order, country_details_master):
    """
    This function inspects the best country guess for a CUSIP associated with a blank country code.
    
    Parameters:
        row: The input dataframe row
        source_preference_order: Arbitrary source preference ordering (dictionary)
        country_details_master: A dictionary listing each source's country assignment for a given CUSIP6

    Returns: 
        (str: country_fallback_code, str: e)
    """

    # FIXME
    if issuer_num_up not in country_details_master.keys():
        return ("", "L")

    assert issuer_num_up in country_details_master.keys()
    country_details = country_details_master[issuer_num_up]
    source_preference_order_inv = {v:k for k,v in source_preference_order.items()}

    if (country_details['country_{}'.format(source_preference_order_inv[1])] != "" and 
            country_details['country_{}'.format(source_preference_order_inv[1])] not in tax_havens):
        return (country_details['country_{}'.format(source_preference_order_inv[1])], "A")

    elif (country_details['country_{}'.format(source_preference_order_inv[2])] != "" and 
            country_details['country_{}'.format(source_preference_order_inv[2])] not in tax_havens):
        return (country_details['country_{}'.format(source_preference_order_inv[2])], "B")

    elif (country_details['country_{}'.format(source_preference_order_inv[3])] != "" and 
            country_details['country_{}'.format(source_preference_order_inv[3])] not in tax_havens):
        return (country_details['country_{}'.format(source_preference_order_inv[3])], "C")

    elif (country_details['country_{}'.format(source_preference_order_inv[4])] != "" and 
            country_details['country_{}'.format(source_preference_order_inv[4])] not in tax_havens):
        return (country_details['country_{}'.format(source_preference_order_inv[4])], "D")

    elif (country_details['country_{}'.format(source_preference_order_inv[4])] != "" and 
            country_details['country_{}'.format(source_preference_order_inv[4])] not in tax_havens):
        return (country_details['country_{}'.format(source_preference_order_inv[4])], "E")

    elif country_details['country_ai'] != "" and country_details['country_ai'] not in tax_havens:
        return (country_details['country_ai'], "F")

    elif country_details['country_ms_ai'] != "" and country_details['country_ms_ai'] not in tax_havens:
        return (country_details['country_ms_ai'], "G")

    elif country_details['cgs_domicile'] != "" and country_details['cgs_domicile'] not in tax_havens:
        return (country_details['cgs_domicile'], "H")

    elif country_details['country_ms'] != "" and country_details['country_ms'] not in tax_havens:
        return (country_details['country_ms'], "I")

    elif country_details['cgs_domicile'] != "":
        return (country_details['cgs_domicile'], "J")

    else:
        return ("", "K")


# Main routine
if __name__ == "__main__":

    ##########################################################################################
    # Initial I/O steps
    ##########################################################################################

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datapath", type=str, help="MNS data path")
    args = parser.parse_args()
    mns_data = args.datapath
    
    # Set up logging
    logger = logging.getLogger(__name__)
    logfile = '{}/results/logs/{}_Ultimate_Parent_Aggregation.log'.format(mns_data, getpass.getuser())
    if os.path.exists(logfile):
        os.remove(logfile)
    logging.basicConfig(filename=logfile, filemode='w', level=logging.DEBUG)
    sys.stderr = open(logfile, 'a')
    sys.stdout = open(logfile, 'a')
    logger.info("Begin Ultimate_Parent_Aggregation")

    # Load datasets
    logger.info("Reading data")
    ms_modal_country = pd.read_stata(os.path.join(mns_data, "temp/Internal_Country_NonUS_US.dta"))
    orbis = pd.read_stata(os.path.join(mns_data, "temp/orbis/bvd_up_compact_latest_only.dta"))
    sdc = pd.read_stata(os.path.join(mns_data, "temp/SDC/datasets/sdc_cusip6_bg.dta"))
    sdc_country = pd.read_stata(os.path.join(mns_data, "temp/SDC/additional/cusip6_country_sdc.dta"))
    cgs = pd.read_stata(os.path.join(mns_data, "temp/cgs/cgs_compact_complete.dta"))
    cgs_ai = pd.read_stata(os.path.join(mns_data, "temp/cgs/ai_master_for_up_aggregation.dta"))
    ciq = pd.read_stata(os.path.join(mns_data, "temp/ciq/ciq_bg_cusip.dta"))
    dealogic = pd.read_stata(os.path.join(mns_data, "temp/dealogic/dealogic_aggregation_file.dta"))
    factset = pd.read_stata(os.path.join(mns_data, "temp/factset/factset_cusip6_bg.dta"))

    # Adjust names
    ms_modal_country = ms_modal_country.rename(columns={'iso_country_code': 'ms_country'})
    cgs = cgs.rename(columns={'domicile': 'cgs_domicile'})

    # Load additional company names datasets
    ciq_names = pd.read_stata(os.path.join(mns_data, "temp/ciq/ciq_up_names.dta"), columns=['cusip6', 'companyname']).drop_duplicates()
    sdc_names = pd.read_stata(os.path.join(mns_data, "temp/SDC/datasets/sdc_appended.dta"), columns=['CUSIP', 'Issuer']).drop_duplicates()
    dlg_names = pd.read_stata(os.path.join(mns_data, "temp/dealogic/dealogic_aggregation_file.dta"), columns=['CUSIP', 'name']).drop_duplicates()
    sdc_names = sdc_names[sdc_names.CUSIP != ""]
    ciq_names = ciq_names[ciq_names.cusip6 != ""]
    dlg_names = dlg_names[dlg_names.CUSIP != ""]

    # Manual corrections to XSN codes
    orbis.guo50_iso_country_code = orbis.guo50_iso_country_code.replace("II", "XSN")

    # Apply corrections
    from UP_Manual_Corrections import drop_cusip6
    ciq = ciq[~ciq.cusip6.isin(drop_cusip6['ciq'])]
    dealogic = dealogic[~dealogic.CUSIP.isin(drop_cusip6['dlg'])]
    sdc = sdc[~sdc.cusip6.isin(drop_cusip6['sdc'])]
    orbis = orbis[~orbis.bvdid_cusip6.isin(drop_cusip6['bvd'])]
    factset = factset[~factset.cusip6.isin(drop_cusip6['fds'])]

    ##########################################################################################
    # Stationary transformation of SDC, CIQ, ORBIS, CGS AI, Dealogic
    ##########################################################################################

    # Make everything stationary
    logger.info("Stationary transformation: SDC")
    sdc = flatten_parent_child_map(sdc, "cusip6", "cusip6_bg", logger)
    logger.info("Stationary transformation: CIQ")
    ciq_stationary = flatten_parent_child_map(ciq, "cusip6", "ciqup_cusip6", logger)
    logger.info("Stationary transformation: BVD")
    orbis_stationary = flatten_parent_child_map(orbis, "bvdid_cusip6", "guo50_cusip6", logger)
    logger.info("Stationary transformation: AI")
    cgs_ai_stationary = flatten_parent_child_map(cgs_ai, "issuer_number", "ai_parent_issuer_num", logger)
    logger.info("Stationary transformation: Dealogic")
    dealogic_stationary = flatten_parent_child_map(dealogic, "CUSIP", "p_CUSIP", logger)
    logger.info("Stationary transformation: Factset")
    factset_stationary = flatten_parent_child_map(factset, "cusip6", "cusip6_bg", logger)

    # Merge back with original details
    ciq = ciq_stationary.merge(ciq[['ciqup_cusip6', 'ciq_country_bg']].drop_duplicates(), on="ciqup_cusip6", how="left")
    orbis = orbis_stationary.merge(orbis[['guo50_cusip6', 'guo50_iso_country_code']].drop_duplicates(), on="guo50_cusip6", how="left")
    cgs_ai = cgs_ai_stationary.merge(cgs_ai[['ai_parent_issuer_num', 'ai_parent_domicile']].drop_duplicates(), on="ai_parent_issuer_num", how="left")
    dealogic = dealogic_stationary.merge(dealogic[['p_CUSIP', 'p_nationalityofbusinessisocode']].drop_duplicates(), on="p_CUSIP", how="left")
    factset = factset_stationary.merge(factset[['cusip6_bg', 'country_bg']].drop_duplicates(), on="cusip6_bg", how="left")

    ##########################################################################################
    # Merging all the data sources
    ##########################################################################################

    # Initial processing the datasets
    logger.info("Initial processing of datasets")
    sdc = sdc.merge(sdc_country[['cusip6', 'iso_country_code']], left_on="cusip6_bg", right_on="cusip6", how="left", suffixes=("", "_y"))
    sdc = sdc.drop(['cusip6_y'], axis=1)
    cgs = cgs[['issuer_number', 'cgs_domicile', 'issuer_name']]
    sdc = sdc.rename(columns={'iso_country_code': 'sdc_iso_country_code', 'cusip6_bg': 'sdc_cusip6_bg', 'cusip6': 'sdc_cusip6'})
    cgs = cgs[cgs.issuer_number != ""]
    dealogic = dealogic.rename(columns={'p_nationalityofbusinessisocode': 'dlg_iso_country_code', 'p_CUSIP': 'dlg_cusip6_bg', 'CUSIP': 'dlg_cusip6'})
    dealogic[dealogic.dlg_cusip6 != ""]
    factset = factset.rename(columns={'cusip6_bg': 'fds_cusip6_bg', 'country_bg': 'country_fds'})
    factset = factset[factset.cusip6 != ""]
    cgs = cgs.drop_duplicates("issuer_number")
    cgs_ai = cgs_ai.drop_duplicates("issuer_number")
    ciq = ciq.drop_duplicates("cusip6")
    ms_modal_country = ms_modal_country.drop_duplicates("cusip6")
    sdc = sdc.drop_duplicates("sdc_cusip6")
    orbis = orbis.drop_duplicates("bvdid_cusip6")
    dealogic = dealogic.drop_duplicates("dlg_cusip6")
    factset = factset.drop_duplicates("cusip6")

    # Merge all data
    logger.info("Merging all data sources")
    step1 = cgs.merge(ciq.rename(columns={'cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")
    step2 = step1.merge(cgs_ai[["issuer_number", "ai_parent_issuer_num", "ai_parent_domicile"]],how="outer", on="issuer_number").fillna("")
    step3 = step2.merge(ms_modal_country[['cusip6', 'ms_country']].rename(columns={'cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")
    step4 = step3.merge(sdc.rename(columns={'sdc_cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")
    step4b = step4.merge(orbis[['bvdid_cusip6', 'guo50_cusip6', 'guo50_iso_country_code']].rename(columns={'bvdid_cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")
    step4c = step4b.merge(dealogic.rename(columns={'dlg_cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")
    step5 = step4c.merge(factset.rename(columns={'cusip6': 'issuer_number'}), how="outer", on="issuer_number").fillna("")

    # Renaming columns and adjustments
    step5["country_bg"] = ""
    step5 = step5[step5.issuer_number != "\x1a"]
    step5 = step5.rename(columns={
        "ciqup_cusip6": "cusip6_up_ciq",
        "ciq_country_bg": "country_ciq",
        "ai_parent_issuer_num": "cusip6_up_ai",
        "ai_parent_domicile": "country_ai",
        "ms_country": "country_ms",
        "sdc_cusip6_bg": "cusip6_up_sdc",
        "dlg_cusip6_bg": "cusip6_up_dlg",
        "fds_cusip6_bg": "cusip6_up_fds",
        "dlg_iso_country_code": "country_dlg",
        "sdc_iso_country_code": "country_sdc",
        "guo50_cusip6": "cusip6_up_bvd",
        "guo50_iso_country_code": "country_bvd"
    })

    ##########################################################################################
    # Constructing modal reports for AI issuers
    ##########################################################################################

    # Find modal MS reports for associated issuers (we give priority to modal report for AI issuer number if
    # this is non-FP, else we use the modal report for the AI's issuing subsidiaries)
    logger.info("Finding modal MS reports for associated issuers")
    iso_unique_ai = pd.DataFrame(step5[(step5.cusip6_up_ai != "") & (step5.country_ms != "")].groupby("cusip6_up_ai")['country_ms'].unique())
    iso_unique_ai_l2 = iso_unique_ai[list(map(lambda x: len(x) > 1, iso_unique_ai.country_ms))]
    iso_unique_ai_l1 = iso_unique_ai[list(map(lambda x: len(x) == 1, iso_unique_ai.country_ms))].reset_index()
    iso_unique_ai_l1['country_ms'] = list(map(lambda x: x[0], iso_unique_ai_l1.country_ms))

    # Consolidate MS modal reports for AI codes
    logger.info("Consolidating modal MS reports for associated issuers")
    step6 = step5.merge(
            step5[['issuer_number', 'country_ms']], left_on="cusip6_up_ai", right_on="issuer_number", suffixes=("", "_ai"), how="left"
        ).merge(
            iso_unique_ai_l1, on="cusip6_up_ai", suffixes=("", "_ai2"), how="left"
    )
    step6['country_ms_ai'] = step6['country_ms_ai'].fillna("")
    step6['country_ms_ai2'] = step6['country_ms_ai2'].fillna("")
    step6['replace_country_ms_ai'] = step6.apply(
        lambda row: (row.country_ms_ai == "") or (row.country_ms_ai in tax_havens and row.country_ms_ai2 not in tax_havens),
        axis=1
    )
    step6.loc[step6.replace_country_ms_ai == True, "country_ms_ai"] = step6.loc[step6.replace_country_ms_ai == True, "country_ms_ai2"]
    step6 = step6.drop(['country_ms_ai2', 'replace_country_ms_ai', 'issuer_number_ai'], axis=1)

    # At this point, country_ms_ai in step6 is the modal MS report for the security's associated issuer,
    # inclusive of the FP correction made above

    ##########################################################################################
    # Linking observations to CIQ, SDC, BVD, DLG, FDS via their associated issuer code if match not
    # possible via their own CUSIP6 
    ##########################################################################################

    # Use CIQ, SDC, BVD, DLG, FDS ultimate parent codes for associated issuer if not available for own CUSIP
    # Flag 2 indicates that we overwrite with a UP CUSIP6 that is missing a country code
    def gen_source_replacement_flag(row, source):
        if row['cusip6_up_{}_ai'.format(source)] == "" or row['country_{}_ai'.format(source)] == "":
            return 0
        elif row['country_{}_ai'.format(source)] == "" and row['country_{}'.format(source)] != "":
            return 2
        elif row['cusip6_up_{}'.format(source)] == "" and row['country_{}'.format(source)] == "":
            return 1
        elif row['country_{}_ai'.format(source)] not in tax_havens and row['country_{}'.format(source)] in tax_havens:
            return 1
        else:
            return 0

    # Perform step 7 updating
    logger.info("Linking CIQ, SDC, BVD, DLG, FDS ultimate parent codes for associated issuer if not available for own CUSIP")
    step7 = step6.merge(
            step6[['issuer_number', 'cusip6_up_ciq', 'country_ciq', 'cusip6_up_sdc', 'country_sdc', 
                   'cusip6_up_bvd', 'country_bvd', 'cusip6_up_dlg', 'country_dlg',  'cusip6_up_fds', 'country_fds']], 
            left_on="cusip6_up_ai", right_on="issuer_number", suffixes=("", "_ai"), how="left"
        ).fillna("")
    for source in ['ciq', 'sdc', 'bvd', 'dlg', 'fds']:
        logger.info("Processing source {}".format(source))
        step7['replace_{}'.format(source)] = step7.apply(lambda x: gen_source_replacement_flag(x, source), axis=1)
        step7['cusip6_up_{}_orig'.format(source)] = step7['cusip6_up_{}'.format(source)]
        step7['country_{}_orig'.format(source)] = step7['country_{}'.format(source)]
        step7.loc[step7['replace_{}'.format(source)].isin([1,2]), "cusip6_up_{}".format(source)] = step7.loc[step7['replace_{}'.format(source)].isin([1,2]), "cusip6_up_{}_ai".format(source)] 
        step7.loc[step7['replace_{}'.format(source)].isin([1,2]), "country_{}".format(source)] = step7.loc[step7['replace_{}'.format(source)].isin([1,2]), "country_{}_ai".format(source)]
    step7 = step7.drop(['issuer_number_ai', 'cusip6_up_ciq_ai', 'country_ciq_ai', 'cusip6_up_sdc_ai', 'country_sdc_ai', 'cusip6_up_dlg_ai', 'country_dlg_ai', 
                        'cusip6_up_bvd_ai', 'country_bvd_ai', 'cusip6_up_fds_ai', 'country_fds_ai', 'replace_ciq', 'replace_sdc', 'replace_bvd', 'replace_dlg', 
                        'replace_fds'], axis=1)

    ##########################################################################################
    # Harmonizing country assignments in BVD, CIQ, SDC, DLG, FDS for each CUSIP6
    ##########################################################################################

    # Harmonize country reports for UPs from BVD, CIQ, SDC, DLG, FDS
    logger.info("Harmonizing country reports for UPs from BVD, CIQ, SDC, DLG, FDS")
    cup_countries = pd.DataFrame({'cusip6_up': [x for x in set(step7.cusip6_up_ciq) | set(step7.cusip6_up_sdc) | 
        set(step7.cusip6_up_bvd) | set(step7.cusip6_up_dlg) | set(step7.cusip6_up_fds) if x != ""]})
    cup_countries = cup_countries.merge(step5[['cusip6_up_ciq', 'country_ciq']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_ciq', how='left')
    cup_countries = cup_countries.merge(step5[['cusip6_up_sdc', 'country_sdc']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_sdc', how='left')
    cup_countries = cup_countries.merge(step5[['cusip6_up_bvd', 'country_bvd']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_bvd', how='left')
    cup_countries = cup_countries.merge(step5[['cusip6_up_dlg', 'country_dlg']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_dlg', how='left')
    cup_countries = cup_countries.merge(step5[['cusip6_up_fds', 'country_fds']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_fds', how='left')
    cup_countries = cup_countries.drop(['cusip6_up_ciq', 'cusip6_up_sdc', 'cusip6_up_bvd', 'cusip6_up_dlg', 'cusip6_up_fds'], axis=1).fillna("")
    cup_countries['tot_pres'] = cup_countries.replace("", np.nan).apply(lambda x: x.count() - 1, axis=1)
    cup_countries = cup_countries.merge(step5[['issuer_number', 'country_ms', 'country_ai']].drop_duplicates(), left_on='cusip6_up', right_on='issuer_number', how='left')
    cup_countries = cup_countries.drop(['issuer_number'], axis=1).fillna("")
    cup_countries['unique_iso'] = cup_countries.apply(lambda x: get_unique_iso_with_sources(
        [x.country_ciq, x.country_sdc, x.country_bvd, x.country_dlg, x.country_fds])[0], axis=1)
    cup_countries['unique_iso_sources'] = cup_countries.apply(lambda x: get_unique_iso_with_sources(
        [x.country_ciq, x.country_sdc, x.country_bvd, x.country_dlg, x.country_fds])[1], axis=1)
    cup_countries['n_unique_iso'] = cup_countries.apply(lambda x: len(x.unique_iso), axis=1)

    # Running the function
    logger.info("Resolving country conflicts among UP sources")
    resolved_countries = cup_countries.progress_apply(lambda x: resolve_top_source_country_conflict(x, tax_havens), axis=1)  
    cup_countries['country_conflict_resolved'] = [x[0] for x in resolved_countries]
    cup_countries['used_pref_order_for_conflict_res'] = [x[1] for x in resolved_countries]
    cup_countries['country_conflict_resolution_source'] = [x[2] for x in resolved_countries]
    cup_countries_nonblank = cup_countries[cup_countries.country_conflict_resolved != ""]

    # Sanity checking the resolution procedure
    cc_resolution_sources = pd.DataFrame(cup_countries_nonblank.groupby('cusip6_up')['country_conflict_resolution_source'].unique())
    assert set([len(x) for x in cc_resolution_sources.country_conflict_resolution_source]) == {1}

    # Get CC resolution sources table (these are unique for each cusip6_up)
    cc_resolution_sources = pd.DataFrame(cup_countries_nonblank.groupby('cusip6_up')['country_conflict_resolution_source'].first())

    # Create country details master table
    country_detail_master = pd.DataFrame({'cusip6_up': [x for x in set(step7.cusip6_up_ciq) | set(step7.cusip6_up_sdc) | 
        set(step7.cusip6_up_bvd) | set(step7.cusip6_up_dlg) | set(step7.cusip6_up_fds) if x != ""]})
    country_detail_master = country_detail_master.merge(step5[['cusip6_up_ciq', 'country_ciq']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_ciq', how='left')
    country_detail_master = country_detail_master.merge(step5[['cusip6_up_sdc', 'country_sdc']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_sdc', how='left')
    country_detail_master = country_detail_master.merge(step5[['cusip6_up_bvd', 'country_bvd']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_bvd', how='left')
    country_detail_master = country_detail_master.merge(step5[['cusip6_up_dlg', 'country_dlg']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_dlg', how='left')
    country_detail_master = country_detail_master.merge(step5[['cusip6_up_fds', 'country_fds']].drop_duplicates(), left_on='cusip6_up', right_on='cusip6_up_fds', how='left')
    country_detail_master = country_detail_master.drop(['cusip6_up_ciq', 'cusip6_up_sdc', 'cusip6_up_bvd', 'cusip6_up_dlg', 'cusip6_up_fds'], axis=1).fillna("")
    country_detail_master = country_detail_master.merge(
        step5[['issuer_number', 'country_ms', 'country_ai']].drop_duplicates(), left_on='cusip6_up', right_on='issuer_number', how='outer'
    ).drop(['cusip6_up'], axis=1)
    country_detail_master = country_detail_master.merge(step6[['issuer_number', 'country_ms_ai', 'cgs_domicile']], how="left").fillna("")
    country_detail_master = country_detail_master.set_index("issuer_number").to_dict(orient="index")

    # Now we harmonize the country codes for the UP sources, so that after this step the fields country_* have no conflicts
    step8 = step7.copy()
    step8['used_pf_for_conflict_res'] = False

    # Harmonization, step 1: Orbis
    step8 = step8.merge(
        cup_countries_nonblank[['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res']],
        left_on="cusip6_up_bvd", right_on="cusip6_up", how="left"
    ).fillna("")
    step8.loc[(step8.used_pref_order_for_conflict_res == True), "used_pf_for_conflict_res"] = True
    step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_bvd != step8.country_conflict_resolved), "country_bvd"] = step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_bvd != step8.country_conflict_resolved), "country_conflict_resolved"]
    step8 = step8.drop(['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res'], axis=1)

    # Harmonization, step 2: SDC
    step8 = step8.merge(
        cup_countries_nonblank[['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res']],
        left_on="cusip6_up_sdc", right_on="cusip6_up", how="left"
    ).fillna("")
    step8.loc[(step8.used_pref_order_for_conflict_res == True), "used_pf_for_conflict_res"] = True
    step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_sdc != step8.country_conflict_resolved), "country_sdc"] = step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_sdc != step8.country_conflict_resolved), "country_conflict_resolved"]
    step8 = step8.drop(['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res'], axis=1)

    # Harmonization, step 3: Orbis
    step8 = step8.merge(
        cup_countries_nonblank[['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res']],
        left_on="cusip6_up_ciq", right_on="cusip6_up", how="left"
    ).fillna("")
    step8.loc[(step8.used_pref_order_for_conflict_res == True), "used_pf_for_conflict_res"] = True
    step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_ciq != step8.country_conflict_resolved), "country_ciq"] = step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_ciq != step8.country_conflict_resolved), "country_conflict_resolved"]
    step8 = step8.drop(['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res'], axis=1)

    # Harmonization, step 4: Dealogic
    step8 = step8.merge(
        cup_countries_nonblank[['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res']],
        left_on="cusip6_up_dlg", right_on="cusip6_up", how="left"
    ).fillna("")
    step8.loc[(step8.used_pref_order_for_conflict_res == True), "used_pf_for_conflict_res"] = True
    step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_dlg != step8.country_conflict_resolved), "country_dlg"] = step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_dlg != step8.country_conflict_resolved), "country_conflict_resolved"]
    step8 = step8.drop(['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res'], axis=1)

    # Harmonization, step 5: Factset
    step8 = step8.merge(
        cup_countries_nonblank[['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res']],
        left_on="cusip6_up_fds", right_on="cusip6_up", how="left"
    ).fillna("")
    step8.loc[(step8.used_pref_order_for_conflict_res == True), "used_pf_for_conflict_res"] = True
    step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_fds != step8.country_conflict_resolved), "country_fds"] = step8.loc[(step8.country_conflict_resolved != "") & 
               (step8.country_fds != step8.country_conflict_resolved), "country_conflict_resolved"]
    step8 = step8.drop(['cusip6_up', 'country_conflict_resolved', 'used_pref_order_for_conflict_res'], axis=1)


    ##########################################################################################
    # Resolving ownership chains among BVD, SDC, CIQ, DLG, FDS
    ##########################################################################################

    logger.info("Resolving ownership chains among UP sources")

    # Ownership chain resolution, step 1: CIQ
    chain_tmp_df = step8.merge(
        step8[['issuer_number', 'cusip6_up_sdc', 'country_sdc', 'cusip6_up_bvd', 'country_bvd', 'cusip6_up_dlg', 'country_dlg', 'cusip6_up_fds', 'country_fds']],
        left_on='cusip6_up_ciq', right_on='issuer_number', suffixes=("", "_y"), how="left"
    ).fillna("")

    ciq_overwrites = chain_tmp_df[(chain_tmp_df.cusip6_up_ciq != "") & (
            ((chain_tmp_df.cusip6_up_sdc_y != "") & (chain_tmp_df.cusip6_up_ciq != chain_tmp_df.cusip6_up_sdc_y)) | 
            ((chain_tmp_df.cusip6_up_bvd_y != "") & (chain_tmp_df.cusip6_up_ciq != chain_tmp_df.cusip6_up_bvd_y)) |
            ((chain_tmp_df.cusip6_up_dlg_y != "") & (chain_tmp_df.cusip6_up_ciq != chain_tmp_df.cusip6_up_dlg_y)) |
            ((chain_tmp_df.cusip6_up_fds_y != "") & (chain_tmp_df.cusip6_up_ciq != chain_tmp_df.cusip6_up_fds_y)) 
    )].drop_duplicates(subset=['cusip6_up_ciq'])

    # Ownership chain resolution, step 2: BVD
    chain_tmp_df = step8.merge(
        step8[['issuer_number', 'cusip6_up_sdc', 'country_sdc', 'cusip6_up_ciq', 'country_ciq', 'cusip6_up_dlg', 'country_dlg', 'cusip6_up_fds', 'country_fds']],
        left_on='cusip6_up_bvd', right_on='issuer_number', suffixes=("", "_y"), how="left"
    ).fillna("")

    bvd_overwrites = chain_tmp_df[(chain_tmp_df.cusip6_up_bvd != "") & (
            ((chain_tmp_df.cusip6_up_sdc_y != "") & (chain_tmp_df.cusip6_up_bvd != chain_tmp_df.cusip6_up_sdc_y)) | 
            ((chain_tmp_df.cusip6_up_ciq_y != "") & (chain_tmp_df.cusip6_up_bvd != chain_tmp_df.cusip6_up_ciq_y)) |
            ((chain_tmp_df.cusip6_up_dlg_y != "") & (chain_tmp_df.cusip6_up_bvd != chain_tmp_df.cusip6_up_dlg_y)) |
            ((chain_tmp_df.cusip6_up_fds_y != "") & (chain_tmp_df.cusip6_up_bvd != chain_tmp_df.cusip6_up_fds_y))
    )].drop_duplicates(subset=['cusip6_up_bvd'])

    # Ownership chain resolution, step 3: SDC
    chain_tmp_df = step8.merge(
        step8[['issuer_number', 'cusip6_up_bvd', 'country_bvd', 'cusip6_up_ciq', 'country_ciq', 'cusip6_up_dlg', 'country_dlg', 'cusip6_up_fds', 'country_fds']],
        left_on='cusip6_up_sdc', right_on='issuer_number', suffixes=("", "_y"), how="left"
    ).fillna("")

    sdc_overwrites = chain_tmp_df[(chain_tmp_df.cusip6_up_sdc != "") & (
            ((chain_tmp_df.cusip6_up_bvd_y != "") & (chain_tmp_df.cusip6_up_sdc != chain_tmp_df.cusip6_up_bvd_y)) | 
            ((chain_tmp_df.cusip6_up_dlg_y != "") & (chain_tmp_df.cusip6_up_sdc != chain_tmp_df.cusip6_up_dlg_y)) | 
            ((chain_tmp_df.cusip6_up_ciq_y != "") & (chain_tmp_df.cusip6_up_sdc != chain_tmp_df.cusip6_up_ciq_y)) | 
            ((chain_tmp_df.cusip6_up_fds_y != "") & (chain_tmp_df.cusip6_up_sdc != chain_tmp_df.cusip6_up_fds_y))
    )].drop_duplicates(subset=['cusip6_up_sdc'])

    # Ownership chain resolution, step 4: Dealogic
    chain_tmp_df = step8.merge(
        step8[['issuer_number', 'cusip6_up_bvd', 'country_bvd', 'cusip6_up_ciq', 'country_ciq', 'cusip6_up_sdc', 'country_sdc', 'cusip6_up_fds', 'country_fds']],
        left_on='cusip6_up_dlg', right_on='issuer_number', suffixes=("", "_y"), how="left"
    ).fillna("")

    dlg_overwrites = chain_tmp_df[(chain_tmp_df.cusip6_up_dlg != "") & (
            ((chain_tmp_df.cusip6_up_bvd_y != "") & (chain_tmp_df.cusip6_up_dlg != chain_tmp_df.cusip6_up_bvd_y)) | 
            ((chain_tmp_df.cusip6_up_sdc_y != "") & (chain_tmp_df.cusip6_up_dlg != chain_tmp_df.cusip6_up_sdc_y)) | 
            ((chain_tmp_df.cusip6_up_ciq_y != "") & (chain_tmp_df.cusip6_up_dlg != chain_tmp_df.cusip6_up_ciq_y)) | 
            ((chain_tmp_df.cusip6_up_fds_y != "") & (chain_tmp_df.cusip6_up_dlg != chain_tmp_df.cusip6_up_fds_y))
    )].drop_duplicates(subset=['cusip6_up_dlg'])

    # Ownership chain resolution, step 5: Factset
    chain_tmp_df = step8.merge(
        step8[['issuer_number', 'cusip6_up_bvd', 'country_bvd', 'cusip6_up_ciq', 'country_ciq', 'cusip6_up_sdc', 'country_sdc', 'cusip6_up_dlg', 'country_dlg']],
        left_on='cusip6_up_fds', right_on='issuer_number', suffixes=("", "_y"), how="left"
    ).fillna("")

    fds_overwrites = chain_tmp_df[(chain_tmp_df.cusip6_up_fds != "") & (
            ((chain_tmp_df.cusip6_up_bvd_y != "") & (chain_tmp_df.cusip6_up_fds != chain_tmp_df.cusip6_up_bvd_y)) | 
            ((chain_tmp_df.cusip6_up_sdc_y != "") & (chain_tmp_df.cusip6_up_fds != chain_tmp_df.cusip6_up_sdc_y)) | 
            ((chain_tmp_df.cusip6_up_ciq_y != "") & (chain_tmp_df.cusip6_up_fds != chain_tmp_df.cusip6_up_ciq_y)) | 
            ((chain_tmp_df.cusip6_up_dlg_y != "") & (chain_tmp_df.cusip6_up_fds != chain_tmp_df.cusip6_up_dlg_y))
    )].drop_duplicates(subset=['cusip6_up_fds'])

    # Some useful objects
    overwrites = {
        'ciq': ciq_overwrites,
        'bvd': bvd_overwrites,
        'sdc': sdc_overwrites,
        'dlg': dlg_overwrites,
        'fds': fds_overwrites
    }
    complementary_sources = {
        'ciq': ['bvd', 'sdc', 'dlg', 'fds'],
        'bvd': ['sdc', 'ciq', 'dlg', 'fds'],
        'sdc': ['bvd', 'ciq', 'dlg', 'fds'],
        'dlg': ['ciq', 'bvd', 'sdc', 'fds'],
        'fds': ['ciq', 'bvd', 'sdc', 'dlg']
    }

    # Resolve cross-source ownership chains
    step9 = resolve_cross_ownership_chains(step8, overwrites, complementary_sources, source_preference_order, logger)

    ##########################################################################################
    # Running the aggregation function
    ##########################################################################################

    # Run the aggregation
    logger.info("Running final aggregation")
    assignment_results = step9.progress_apply(lambda x:
        determine_ultimate_parent_and_country_assignment(x, source_preference_order, country_detail_master), axis=1)
    cusip6_up_bg = [x[0] for x in assignment_results]
    country_bg = [x[1] for x in assignment_results]
    status_codes = [x[2] for x in assignment_results]
    cusip6_up_bg_source = [x[3] for x in assignment_results]
    country_bg_source = [x[4] for x in assignment_results]
    additional_info = [x[5] for x in assignment_results]

    # Add the output to the dataframe
    logger.info("Storing results")
    step9['cusip6_up_bg'] = cusip6_up_bg
    step9['aggregation_case_code'] = status_codes
    step9['country_bg'] = country_bg
    step9['aggregation_additional_info'] = additional_info
    step9['cusip6_up_bg_source'] = cusip6_up_bg_source
    step9['country_bg_source'] = country_bg_source

    # Adjust corner cases
    countries_resolve = uniques[uniques.n_us > 1].reset_index()
    def final_country_resolve(row):
        nfp = [x for x in row.country_bg if x not in tax_havens]
        if len(nfp) == 1:
            return nfp[0]
        else:
            return country_detail_master[row.cusip6_up_bg]['cgs_domicile']
    countries_resolve['bg_resolved'] = countries_resolve.apply(final_country_resolve, axis=1)
    countries_resolve = countries_resolve[['cusip6_up_bg', 'bg_resolved']]
    step95 = step9.merge(countries_resolve, how="left", on="cusip6_up_bg")
    step95.loc[~pd.isna(step95.bg_resolved), "country_bg"] = step95.loc[~pd.isna(step95.bg_resolved), "bg_resolved"]
    step95 = step95.drop(columns=['bg_resolved'])

    ##########################################################################################
    # Reconstructing company names
    ##########################################################################################

    # Add company names from CGS master file
    step10 = step95.merge(cgs[['issuer_number', 'issuer_name']].rename(columns={'issuer_number': 'cusip6_up_bg'}).drop_duplicates(), 
                         on="cusip6_up_bg", suffixes=("", "_up"), how="left")
    step10['issuer_name_up'] = step10['issuer_name_up'].fillna("")

    # If the SDC, CIQ names datasets have multiple entries associated with a single CUSIP6,
    # for simplicity we take the first.
    ciq_names_flat = ciq_names[ciq_names.companyname != ""]
    ciq_names_flat = ciq_names_flat.groupby('cusip6').head(1)
    sdc_names_flat = sdc_names[sdc_names.Issuer != ""]
    sdc_names_flat = sdc_names_flat.groupby('CUSIP').head(1)

    # If not available in CGS master file, resort to company names from SDC and CIQ
    step10 = step10.merge(ciq_names_flat.rename(columns={'cusip6': 'cusip6_up_bg'}), on="cusip6_up_bg", how="left")
    step10 = step10.merge(sdc_names_flat.rename(columns={'CUSIP': 'cusip6_up_bg'}), on="cusip6_up_bg", how="left")
    step10.Issuer = step10.Issuer.fillna("").str.upper()
    step10.companyname = step10.companyname.fillna("").str.upper()
    step10.loc[(step10.issuer_name_up == ""), "issuer_name_up"] = step10.loc[(step10.issuer_name_up == ""), "Issuer"]
    step10.loc[(step10.issuer_name_up == ""), "issuer_name_up"] = step10.loc[(step10.issuer_name_up == ""), "companyname"]
    step10 = step10.drop(['Issuer', 'companyname'], axis=1)

    ##########################################################################################
    # Final adjustments and output
    ##########################################################################################

    # Final adjustments: if country code was updated during source harmonization, reflect this in final output
    step10 = step10.merge(cc_resolution_sources.reset_index(), left_on="cusip6_up_bg", right_on="cusip6_up", how="left").drop(["cusip6_up"], axis=1)
    step10.country_conflict_resolution_source = step10.country_conflict_resolution_source.fillna("")
    step10.loc[step10.country_conflict_resolution_source != "", "country_bg_source"] = step10.loc[
        step10.country_conflict_resolution_source != "", "country_conflict_resolution_source"]
    step10 = step10.drop(['country_conflict_resolution_source'], axis=1)
    step10.loc[step10.country_bg == "", "country_bg_source"] = "no non-blank source present"

    # Manual plug for XSN
    step10.loc[step10.cgs_domicile == "XSN", "country_bg"] = step10.loc[step10.cgs_domicile == "XSN", "cgs_domicile"]
    step10.loc[step10.cgs_domicile == "XSN", "cusip6_up_bg"] = step10.loc[step10.cgs_domicile == "XSN", "issuer_number"]
    step10.loc[step10.cgs_domicile == "XSN", "issuer_name_up"] = step10.loc[step10.cgs_domicile == "XSN", "issuer_name"]
    step10.loc[step10.cgs_domicile == "XSN", "cusip6_up_bg_source"] = "xsn_override"
    step10.loc[step10.cgs_domicile == "XSN", "country_bg_source"] = "xsn_override"

    # Ensure all columns are of the appropriate types
    str_cols = ['issuer_number', 'cgs_domicile', 'issuer_name', 'cusip6_up_ciq',
           'country_ciq', 'cusip6_up_ai', 'country_ai', 'country_ms',
           'cusip6_up_sdc', 'country_sdc', 'cusip6_up_bvd', 'country_bvd', 'cusip6_up_dlg', 
           'country_dlg', 'cusip6_up_dlg_orig', 'country_dlg_orig', 'overwrite_source_dlg',
           'cusip6_up_fds', 'country_fds', 'cusip6_up_fds_orig', 'country_fds_orig', 'overwrite_source_fds',
           'country_bg', 'country_ms_ai', 'cusip6_up_ciq_orig', 'country_ciq_orig',
           'cusip6_up_sdc_orig', 'country_sdc_orig', 'cusip6_up_bvd_orig',
           'country_bvd_orig', 'overwrite_source_bvd', 'overwrite_source_ciq', 
           'overwrite_source_sdc', 'cusip6_up_bg_source', 'country_bg_source',
           'cusip6_up_bg', 'issuer_name_up', 'aggregation_additional_info']

    other_cols = ['aggregation_case_code', 'used_pref_ordering_for_up_resolution', 'used_pf_for_conflict_res']

    for scol in str_cols:
        step10[scol] = step10[scol].astype(str)

    for scol in ['used_pref_ordering_for_up_resolution', 'used_pf_for_conflict_res']:
        step10[scol] = step10[scol].astype(bool)

    # Rename columns whose names are too long
    step10 = step10.rename(columns={'used_pref_ordering_for_up_resolution': 'used_pref_order_for_up_res'})

    # Apply corrections
    from UP_Manual_Corrections import extra_link
    for child, parent in extra_link.items():
        print("Processing correction for IN = {}".format(child))
        step10.loc[step10.issuer_number == child, "cusip6_up_bg"] = parent
        bg_df = step10[step10.cusip6_up_bg == parent][['country_bg', 'issuer_name_up', 'country_bg_source']]
        if bg_df.shape[0] > 0:
            step10.loc[step10.issuer_number == child, "country_bg"] = bg_df.iloc[0]['country_bg']
            step10.loc[step10.issuer_number == child, "issuer_name_up"] = bg_df.iloc[0]['issuer_name_up']
            step10.loc[step10.issuer_number == child, "country_bg_source"] = bg_df.iloc[0]['country_bg_source']
            step10.loc[step10.issuer_number == child, "cusip6_up_bg_source"] = "manual_correction"
        else:
            raise Exception("Parent {} not found in step10 data".format(parent))

    # Ensure stationarity
    map_stationary = flatten_parent_child_map(step10, "issuer_number", "cusip6_up_bg", logger)
    step11 = step10[step10.issuer_number.isin(map_stationary.issuer_number)].merge(
    map_stationary.rename(columns={'cusip6_up_bg': 'u_cusip6_up_bg'}), on="issuer_number", how="left")
    terminal_in = step11[step11.cusip6_up_bg != step11.u_cusip6_up_bg].issuer_number
    terminal_up = step11[step11.cusip6_up_bg != step11.u_cusip6_up_bg].u_cusip6_up_bg
    terminal_overwrites = step10[step10.cusip6_up_bg.isin(set(terminal_up))].drop_duplicates(
        'cusip6_up_bg')[['cusip6_up_bg', 'country_bg', 'issuer_name_up', 'country_bg_source']]
    step12 = step11.merge(terminal_overwrites.rename(columns={'cusip6_up_bg': 'u_cusip6_up_bg'}), on='u_cusip6_up_bg', how='left', suffixes=("", "_u"))
    step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "country_bg"] = step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "country_bg_u"]
    step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "issuer_name_up"] = step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "issuer_name_up_u"]
    step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "country_bg_source"] = step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "country_bg_source_u"]
    step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "cusip6_up_bg"] = step12.loc[step12.cusip6_up_bg != step12.u_cusip6_up_bg, "u_cusip6_up_bg"]
    step13 = step12.drop(columns=['u_cusip6_up_bg', 'country_bg_u', 'issuer_name_up_u', 'country_bg_source_u'])
    step13 = step13[step13.issuer_number != ""]
    step13 = step13[step13.issuer_number != "000000"]
    step13 = step13[step13.issuer_number != "#N/A N"]
    
    # Sanity check
    step13_check = step13.merge(
        map_stationary.rename(columns={'cusip6_up_bg': 'u_cusip6_up_bg'}), on="issuer_number", how="left")
    assert step13_check[step13_check.cusip6_up_bg != step13_check.u_cusip6_up_bg].shape[0] == 0

    # Store final output
    step13.to_stata(os.path.join(mns_data, "temp/country_master/up_aggregation_final.dta"), write_index=False)
    compact_cols = ['issuer_number', 'issuer_name', 'cgs_domicile', 'cusip6_up_bg', 'country_bg', 
        'issuer_name_up', 'cusip6_up_bg_source', 'country_bg_source']
    step13[compact_cols].to_stata(os.path.join(mns_data, "temp/country_master/up_aggregation_final_compact.dta"), write_index=False)
