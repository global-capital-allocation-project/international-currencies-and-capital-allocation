# --------------------------------------------------------------------------------------------------
# Ultimate parent aggregation
#
# The files in this folder provide an implementation of the ultimate parent aggregation algorithm of 
# Coppola, Maggiori, Neiman, and Schreger (2019). For a detailed discussion of the aggregation algorithm, 
# please refer to that paper.
#
# This file provides helper methods for the main code in UP_Aggregation.py.
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
from tqdm import tqdm, tqdm_pandas
tqdm.pandas()


# Constant used to indicate cycles
cycle_constant = "__CYCLE__"


# General function for flattening parent-child maps
def flatten_parent_child_map(df, child_col, parent_col, logger):
    """ This function generates a stationary transformation of arbitrary
    child-to-parent mappings.

    Parameters:
        df: The input dataframe
        child_col: Column identifying the child 
        parent_col: Column identifying the parent
        logger: Logger object

    Returns: 
        A dataframe with the flattened child-parent map
    """

    # Helper for stationary transformation
    def _find_stationary_outcome(row, colname, i_max):

        values = list(row[['{}_{}'.format(colname, i) for i in range(i_max)]])
        if values[-1] == values[-2] == values[-3] == values[-4]:
            return values[-1]
        else:
            return cycle_constant

    singletons = list(set(df[parent_col]) - set(df[child_col]))

    extras = pd.DataFrame({
        child_col: singletons,
        parent_col: singletons
    })

    tmp_df = pd.concat(
        [df.dropna(), extras], axis=0, sort=False
    ).rename(
        columns={parent_col: "{}_1".format(parent_col), child_col: "{}_0".format(parent_col)}
    )

    for i in range(2,15):
        tmp_df = tmp_df.merge(
            tmp_df[['{}_{}'.format(parent_col, i-2), "{}_{}".format(parent_col, i-1)]].drop_duplicates(), 
            left_on="{}_{}".format(parent_col, i-1), right_on="{}_{}".format(parent_col, i-2), how="left", 
            suffixes=("", "_{}".format(i))
        ).drop(
            ["{}_{}_{}".format(parent_col, i-2, i)], axis=1
        ).rename(
            columns={"{}_{}_{}".format(parent_col, i-1, i): "{}_{}".format(parent_col, i)}
        )
        
    tmp_df['stationary_outcome'] = tmp_df.progress_apply(lambda x: _find_stationary_outcome(x, parent_col, i), axis=1)
    n_cycles = tmp_df[tmp_df.stationary_outcome == cycle_constant].shape[0]
    logger.info("WARNING: Discarding {} rows out of {} due to cycles".format(n_cycles, df.shape[0]))
    tmp_df = tmp_df[tmp_df.stationary_outcome != cycle_constant][["{}_0".format(parent_col), "stationary_outcome"]].rename(
        columns={"{}_0".format(parent_col): child_col, "stationary_outcome": parent_col}
    )
    return tmp_df


# Graph utility to detect presence cycles in a list of elements
def detect_cycle(lst):
    visited  = []
    for i in range(len(lst)):
        if lst[i] in visited:
            return True
        visited.append(lst[i])
    return False


# Graph utility to detect identity of circular entries in a list of elements
def get_circular_elements(lst):
    visited  = []
    circular = []
    for i in range(len(lst)):
        if lst[i] in visited:
            circular.append(lst[i])
        visited.append(lst[i])
    return list(set(circular))


# Helper function for country conflict resolution
def resolve_top_source_country_conflict(row, tax_havens):
    """Helper function for country conflict resolution among BVD, SDC, CIQ, BVD, FDS

    Parameters:
        row: The input dataframe row
        tax_havens: List of tax haven countries

    Returns: 
        (str: country_code, bool: used_source_preference_ordering, str: source)
    """
    if row.n_unique_iso == 0:
        return ("", False, "")
    
    if row.n_unique_iso == 1 and row.unique_iso[0] in tax_havens:
        return ("", False, "") 
    
    if row.n_unique_iso == 1 and row.unique_iso[0] not in tax_havens: 
        return (row.unique_iso[0], False, row.unique_iso_sources[0])
    
    if row.n_unique_iso == 2:
        x, y = row.unique_iso[0], row.unique_iso[1]
        x_source, y_source = row.unique_iso_sources[0], row.unique_iso_sources[1]
        if x in tax_havens and y not in tax_havens:
            return (y, False, y_source)
        elif x not in tax_havens and y in tax_havens:
            return (x, False, x_source)
        elif x == row.country_ms:
            return (x, False, x_source)
        elif y == row.country_ms:
            return (y, False, y_source)
        elif x == row.country_ai:
            return (x, False, x_source)
        elif y == row.country_ai:
            return (y, False, y_source)
        elif x == row.country_bvd:
            return (x, True, x_source)
        elif y == row.country_bvd:
            return (y, True, y_source)
        elif x == row.country_dlg:
            return (x, True, x_source)
        elif y == row.country_dlg:
            return (y, True, y_source)
        elif x == row.country_fds:
            return (x, True, x_source)
        elif y == row.country_fds:
            return (y, True, y_source)
        elif x == row.country_ciq:
            return (x, True, x_source)
        else:
            return (y, True, y_source)

    if row.n_unique_iso == 3:
        x, y, z = row.unique_iso[0], row.unique_iso[1], row.unique_iso[2]
        x_source, y_source, z_source = row.unique_iso_sources[0], row.unique_iso_sources[1], row.unique_iso_sources[2]
        
        if x not in tax_havens and y in tax_havens and z in tax_havens:
            return (x, False, x_source)
        elif y not in tax_havens and x in tax_havens and z in tax_havens:
            return (y, False, y_source)
        elif z not in tax_havens and y in tax_havens and x in tax_havens:
            return (z, False, z_source)
        elif x == row.country_ms:
            return (x, False, x_source)
        elif y == row.country_ms:
            return (y, False, y_source)
        elif z == row.country_ms:
            return (z, False, z_source)
        elif x == row.country_ai:
            return (x, False, x_source)
        elif y == row.country_ai:
            return (y, False, y_source)
        elif z == row.country_ai:
            return (x, False, z_source)
        elif x == row.country_bvd:
            return (x, True, x_source)
        elif y == row.country_bvd:
            return (x, True, y_source)
        elif z == row.country_bvd:
            return (z, True, z_source)
        elif x == row.country_dlg:
            return (x, True, x_source)
        elif y == row.country_dlg:
            return (x, True, y_source)
        elif z == row.country_dlg:
            return (z, True, z_source)
        elif x == row.country_fds:
            return (x, True, x_source)
        elif y == row.country_fds:
            return (x, True, y_source)
        elif z == row.country_fds:
            return (z, True, z_source)
        elif x == row.country_ciq:
            return (x, True, x_source)
        elif y == row.country_ciq:
            return (x, True, y_source)
        elif z == row.country_ciq:
            return (z, True, z_source)
        elif x == row.country_sdc:
            return (x, True, x_source)
        elif y == row.country_sdc:
            return (x, True, y_source)
        else:
            return (z, True, z_source)

    if row.n_unique_iso == 4:
        w, x, y, z = row.unique_iso[0], row.unique_iso[1], row.unique_iso[2], row.unique_iso[3]
        w_source, x_source, y_source, z_source = row.unique_iso_sources[0], row.unique_iso_sources[1], \
                                                 row.unique_iso_sources[2], row.unique_iso_sources[3]

        if x not in tax_havens and y in tax_havens and z in tax_havens and w in tax_havens:
            return (x, False, x_source)
        elif y not in tax_havens and x in tax_havens and z in tax_havens and w in tax_havens:
            return (y, False, y_source)
        elif z not in tax_havens and y in tax_havens and x in tax_havens and w in tax_havens:
            return (z, False, z_source)
        elif w not in tax_havens and y in tax_havens and x in tax_havens and x in tax_havens:
            return (w, False, w_source)

        elif w == row.country_ms:
            return (w, False, w_source)
        elif x == row.country_ms:
            return (x, False, x_source)
        elif y == row.country_ms:
            return (y, False, y_source)
        elif z == row.country_ms:
            return (x, False, z_source)

        elif w == row.country_ai:
            return (w, False, w_source)
        elif x == row.country_ai:
            return (x, False, x_source)
        elif y == row.country_ai:
            return (y, False, y_source)
        elif z == row.country_ai:
            return (x, False, z_source)

        elif w == row.country_bvd:
            return (w, False, w_source)
        elif x == row.country_bvd:
            return (x, True, x_source)
        elif y == row.country_bvd:
            return (x, True, y_source)
        elif z == row.country_bvd:
            return (z, True, z_source)

        elif w == row.country_dlg:
            return (w, False, w_source)
        elif x == row.country_dlg:
            return (x, True, x_source)
        elif y == row.country_dlg:
            return (x, True, y_source)
        elif z == row.country_dlg:
            return (z, True, z_source)

        elif w == row.country_fds:
            return (w, False, w_source)
        elif x == row.country_fds:
            return (x, True, x_source)
        elif y == row.country_fds:
            return (x, True, y_source)
        elif z == row.country_fds:
            return (z, True, z_source)

        elif w == row.country_ciq:
            return (w, False, w_source)
        elif x == row.country_ciq:
            return (x, True, x_source)
        elif y == row.country_ciq:
            return (x, True, y_source)
        elif z == row.country_ciq:
            return (z, True, z_source)

        elif w == row.country_sdc:
            return (w, False, w_source)
        elif x == row.country_sdc:
            return (x, True, x_source)
        elif y == row.country_sdc:
            return (x, True, y_source)
        else:
            return (z, True, z_source)

    if row.n_unique_iso > 4:
        raise Exception("Got n_unique_iso > 4; please implement")


# Helper function to get unique ISO codes for row
def get_unique_iso_with_sources(country_set):
    unique_iso = []
    iso_sources = []
    source_order = ['ciq', 'sdc', 'bvd', 'dlg', 'fds']
    for i in range(len(source_order)):
        if country_set[i] not in unique_iso and country_set[i] != "":
            unique_iso.append(country_set[i])
            iso_sources.append(source_order[i])
    return unique_iso, iso_sources


# Function to find multiple modes
def get_multiple_modes(array):
    most = max(list(map(array.count, array)))
    return list(set(filter(lambda x: array.count(x) == most, array)))


# Function to resolve cross-source ownership chains
def resolve_cross_ownership_chains(df_in, overwrites, complementary_sources, source_preference_order, logger):
    """ This function resolves cross-source ownership chains.

    Parameters:
        df_in: The input dataframe

    Returns: 
        A dataframe with the resolved ownership chains
    """

    # Resolve entry hierarchy
    resolved_chains = {}
    for source in ['bvd', 'sdc', 'ciq', 'dlg', 'fds']:

        logger.info("Processing source {}".format(source))
        ownership_chain = {}

        for ix in tqdm(range(overwrites[source].shape[0])):

            # Chain structure
            # Elements of the form (cusip6_up, country, source, used_preference_order)
            ownership_chain[ix] = []

            # Some book-keeping
            done = False
            maxiter = 10
            next_source = source
            i = 0

            # Read first-level info
            x_source, y_source, z_source, o_source = (complementary_sources[source][0], complementary_sources[source][1], 
                                                      complementary_sources[source][2], complementary_sources[source][3])
            orig_up_cusip6 = overwrites[source].iloc[ix]['cusip6_up_{}'.format(source)]
            orig_country = overwrites[source].iloc[ix]['country_{}'.format(source)]
            x_res = overwrites[source].iloc[ix]['cusip6_up_{}_y'.format(x_source)]
            y_res = overwrites[source].iloc[ix]['cusip6_up_{}_y'.format(y_source)]
            z_res = overwrites[source].iloc[ix]['cusip6_up_{}_y'.format(z_source)]
            o_res = overwrites[source].iloc[ix]['cusip6_up_{}_y'.format(o_source)]
            x_country = overwrites[source].iloc[ix]['country_{}_y'.format(x_source)]
            y_country = overwrites[source].iloc[ix]['country_{}_y'.format(y_source)]
            z_country = overwrites[source].iloc[ix]['country_{}_y'.format(z_source)]
            o_country = overwrites[source].iloc[ix]['country_{}_y'.format(o_source)]
            ownership_chain[ix].append((orig_up_cusip6, orig_country, source, False))

            while not done and i < maxiter:

                # Read next-level parents
                if i > 0:
                    try:
                        next_row = overwrites[next_source][overwrites[next_source]['cusip6_up_{}'.format(next_source)] == next_cusip].iloc[0]
                    except IndexError:
                        done = True
                        continue
                    orig_up_cusip6 = next_cusip
                    orig_country = next_row['country_{}'.format(next_source)]
                    x_source, y_source, z_source, o_source = (complementary_sources[next_source][0], complementary_sources[next_source][1], 
                                                              complementary_sources[next_source][2], complementary_sources[next_source][3])
                    x_res = next_row['cusip6_up_{}_y'.format(x_source)]
                    y_res = next_row['cusip6_up_{}_y'.format(y_source)]
                    z_res = next_row['cusip6_up_{}_y'.format(z_source)]
                    o_res = next_row['cusip6_up_{}_y'.format(o_source)]
                    x_country = next_row['country_{}_y'.format(x_source)]
                    y_country = next_row['country_{}_y'.format(y_source)]
                    z_country = next_row['country_{}_y'.format(z_source)]
                    o_country = next_row['country_{}_y'.format(o_source)]

                # Check if we have multiple parents
                used_pref_order_l0 = ownership_chain[ix][-1][3]
                if sum([x != "" for x in [x_res, y_res, z_res, o_res]]) > 1 and len(set(x for x in [x_res, y_res, z_res, o_res] if x != "")) > 1:
                    double_parent = True
                    used_pref_order_l0 = True
                else:
                    double_parent = False

                # Determine which source to traverse
                if ((orig_up_cusip6 == x_res and orig_up_cusip6 == y_res and orig_up_cusip6 == z_res and orig_up_cusip6 == o_res) or 
                    (orig_up_cusip6 == x_res and y_res == "" and z_res == "" and o_res == "") or
                    (orig_up_cusip6 == y_res and x_res == "" and z_res == "" and o_res == "") or
                    (orig_up_cusip6 == z_res and x_res == "" and y_res == "" and o_res == "") or
                    (orig_up_cusip6 == o_res and x_res == "" and y_res == "" and z_res == "")):
                    done = True
                elif x_res != "" and y_res == "" and z_res == "" and o_res == "":
                    ownership_chain[ix].append((x_res, x_country, x_source, used_pref_order_l0))
                    next_source = x_source
                    next_cusip = x_res
                elif y_res != "" and x_res == "" and z_res == "" and o_res == "":
                    ownership_chain[ix].append((y_res, y_country, y_source, used_pref_order_l0))
                    next_source = y_source
                    next_cusip = y_res
                elif z_res != "" and y_res == "" and x_res == "" and o_res == "":
                    ownership_chain[ix].append((z_res, z_country, z_source, used_pref_order_l0))
                    next_source = z_source
                    next_cusip = z_res
                elif o_res != "" and y_res == "" and x_res == "" and z_res == "":
                    ownership_chain[ix].append((o_res, o_country, o_source, used_pref_order_l0))
                    next_source = o_source
                    next_cusip = o_res
                else:
                    min_pref_order = min([
                        source_preference_order[x_source], source_preference_order[y_source], source_preference_order[z_source], source_preference_order[o_source]
                    ])
                    if source_preference_order[x_source] == min_pref_order:
                        ownership_chain[ix].append((x_res, x_country, x_source, used_pref_order_l0))
                        next_source = x_source
                        next_cusip = x_res
                    elif source_preference_order[y_source] == min_pref_order:
                        ownership_chain[ix].append((y_res, y_country, y_source, used_pref_order_l0))
                        next_source = y_source
                        next_cusip = y_res
                    elif source_preference_order[z_source] == min_pref_order:
                        ownership_chain[ix].append((z_res, z_country, z_source, used_pref_order_l0))
                        next_source = z_source
                        next_cusip = z_res
                    else:
                        ownership_chain[ix].append((o_res, o_country, o_source, used_pref_order_l0))
                        next_source = o_source
                        next_cusip = o_res
                    
                # Check if done
                if not done and orig_up_cusip6 == next_cusip:
                    done = True

                # Augment counter
                i += 1

        # Final processing
        decycled_ownership_chain = {}
        for ix in tqdm(ownership_chain.keys()):

            # Now break cycles according to preference ordering
            if detect_cycle(ownership_chain[ix]):
                circular = get_circular_elements(ownership_chain[ix])
                pri_index = 100
                preferred_source_ix = -1
                preferred_source = ""
                for i in range(len(circular)):
                    lev_source = circular[i][2]
                    if source_preference_order[lev_source] < pri_index:
                        preferred_source = lev_source
                        preferred_source_ix = i
                resolved_chain = [ownership_chain[ix][0], (circular[preferred_source_ix][0], circular[preferred_source_ix][1], preferred_source, True)]
                chain_final = resolved_chain
            else:
                chain_final = ownership_chain[ix]

            # Now pop tax haven parents until we get to the topmost non-FP element of the chain
            stop_pruning = False
            while len(chain_final) > 0 and not stop_pruning:
                if chain_final[-1][1] not in tax_havens:
                    stop_pruning = True
                else:
                    chain_final.pop()
            decycled_ownership_chain[ix] = chain_final

        resolved_chains[source] = decycled_ownership_chain

    # Perform overwrites from unwound hierarchies
    df_out = df_in.copy()
    df_out['used_pref_ordering_for_up_resolution'] = False
    correspondence_tables = {}

    for source in ['bvd', 'ciq', 'sdc', 'dlg', 'fds']:

        orig_cusip6 = []
        parent_cusip6 = []
        parent_country = []
        overwrite_source = []
        used_pref_ordering_for_parent_resolution = []

        for i in range(len(resolved_chains[source])):
            element = resolved_chains[source][i]
            if len(element) > 0:
                orig_cusip6.append(element[0][0])
                parent_cusip6.append(element[-1][0])
                parent_country.append(element[-1][1])
                overwrite_source.append(element[-1][2])
                used_pref_ordering_for_parent_resolution.append(element[-1][3])

        correspondence_table = pd.DataFrame({
            'orig_cusip6': orig_cusip6,
            'parent_cusip6': parent_cusip6,
            'parent_country': parent_country,
            'overwrite_source': overwrite_source,
            'used_pref_ordering_for_parent_resolution_new': used_pref_ordering_for_parent_resolution
        })

        correspondence_tables[source] = correspondence_table

        assert correspondence_table[correspondence_table.parent_country.isin(tax_havens)].shape[0] == 0
        df_out = df_out.merge(correspondence_table, left_on="cusip6_up_{}".format(source), right_on="orig_cusip6", how="left").fillna("")
        df_out.loc[(df_out.parent_cusip6 != ""), "cusip6_up_{}".format(source)] = df_out.loc[(df_out.parent_cusip6 != ""), "parent_cusip6"]
        df_out.loc[(df_out.parent_cusip6 != ""), "country_{}".format(source)] = df_out.loc[(df_out.parent_cusip6 != ""), "parent_country"]
        df_out.loc[~(df_out.used_pref_ordering_for_up_resolution) & (df_out.used_pref_ordering_for_parent_resolution_new), 
                  "used_pref_ordering_for_up_resolution"] = True
        df_out = df_out.drop(['orig_cusip6', 'parent_cusip6', 'parent_country', 'used_pref_ordering_for_parent_resolution_new'], axis=1)
        df_out = df_out.rename(columns={'overwrite_source': 'overwrite_source_{}'.format(source)})

    return df_out
