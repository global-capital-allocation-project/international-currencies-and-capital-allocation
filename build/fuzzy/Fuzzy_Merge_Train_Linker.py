# --------------------------------------------------------------------------------------------------
# Fuzzy_Merge_Train_Linker
#
# All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
# Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
# do have an identifier. This allows us to assign a CUSIP to the former records via internal
# cross-linkage.
#
# This file defines the linkage schema for perfoming probabilistic record linkage (fuzzy merge)
# between records that have and miss a CUSIP, respectively. It trains a regularized
# logistic regression model to generate pairwise match probabilities. The trained linker
# and data shards are then serialized for distribution to workers over the cluster.
# --------------------------------------------------------------------------------------------------
from __future__ import print_function

import sys
import socket
import platform
import numpy as np
import pandas as pd
import os
import dedupe
import random
import cloudpickle
import argparse
import shutil
import itertools
import getpass

from util.serialization import write_training, read_training, _clean_datapoint, \
    clean_list, split_dict_equally, store_linker, parse_missing_fields, df_to_dict
from rlr import RegularizedLogisticRegression

import logging


# Hardwired settings (useful for debugging/development)
linker_version = 0
serialize_linker = True


# Utility to standardize data format
def standardize_data_types(df):
    df.maturitydate = df.maturitydate.apply(lambda x: x if x != '' else pd.NaT)
    df.maturitydate = df.maturitydate.apply(lambda x: x if x <= pd.Timestamp.max else pd.NaT)
    df.maturitydate = df.maturitydate.apply(lambda x: x if x >= pd.Timestamp.min else pd.NaT)
    df.maturitydate = pd.to_datetime(df.maturitydate)
    df.securityname = df.securityname.astype("str").replace('', np.nan)
    df.coupon = pd.Series([pd.to_numeric(x.replace("..", ".").replace("\\", "").replace("NA", "").lstrip(),
        errors="coerce") if x not in ["", "..", " ", "."] 
    	else np.nan for x in df.coupon]).astype("float32")
    df.iso_country_code = df.iso_country_code.astype("str").replace('', np.nan)
    df.mns_subclass = df.mns_subclass.astype("str").replace('', np.nan)
    df.currency_id = df.currency_id.astype("str").replace('', np.nan)
    return df


# Comparator for coupon field
def coupon_comparator(coupon_x, coupon_y):
    delta = np.min([
        np.abs(coupon_x - coupon_y),
        np.abs(coupon_x * 100. - coupon_y),
        np.abs(coupon_x - coupon_y * 100.)
    ])
    return delta


# Comparator for maturitydate field
def maturitydate_comparator(maturitydate_x, maturitydate_y):
    return np.abs((maturitydate_x - maturitydate_y).days)


# Comparator for categorical fields
def categorical_comparator(cat_x, cat_y):
    if cat_x == cat_y:
        return 0.
    else:
        return 1.


# Comparator for security descriptors
def security_descriptor_comparator(desc_x, desc_y):
    if desc_y == "" or desc_y == "":
        return 0
    return len(set(desc_x.split(", ")).intersection(desc_y.split(", ")))


# Utility to subset the training data by geography
def subset_training_data(training_pairs, geography):
    out = {}
    for pair_type in ['match', 'distinct']:
        out[pair_type] = [x for x in training_pairs_prev[pair_type] 
                              if x[0]['geography'] == geography and x[1]['geography'] == geography]
    return out


# Function for threshold determination via expected F-score maximization
def find_optimal_threshold(linker, bad_data, good_data, n_samples=500, recall_weight=.2):

    # Set random seed
    random.seed(1)

    # Sample data for computing expectations
    sampled_keys = random.sample(bad_data, n_samples)
    bad_data_sample_for_threshold = {k:bad_data[k] for k in sampled_keys}

    # Get all the candidate blocks
    sample_blocks = linker._blockData(bad_data_sample_for_threshold, good_data_dict)
    candidates = itertools.chain.from_iterable(linker._blockedPairs(sample_blocks))

    # Compute match probabilities
    probabilities = []
    for candidate in candidates:
        prob = linker.classifier.predict_proba(
            linker.data_model.distances([(_clean_datapoint(candidate[0][1]), 
                                          _clean_datapoint(candidate[1][1]))])
        )
        probabilities.append(prob)
    probability = np.concatenate(probabilities)
    probability.sort()

    # Compute expected F1 scores
    f1_scores = []
    taus = np.linspace(.7, .995, 100)

    for tau in taus:
        yhat = (probability > tau)
        numerator = np.sum(probability * yhat)
        precision = numerator / np.sum(yhat)
        recall = numerator / np.sum(probability)
        f1 = recall * precision / (recall + recall_weight ** 2 * precision)
        f1_scores.append(f1)
        
    # Return the optimal threshold
    return taus[np.argmax(f1_scores)]


# Utility to store bad data records in a sharded fashion (for distributed linkage jobs)
def store_sharded_data(bad_data_dict, n_chunks, scratch_dir, asset_class, domicile):

    # Shard the bad data
    if domicile == "all":
        data_shards = split_dict_equally(bad_data_dict, chunks = n_chunks)
    elif domicile == "us":
        data_shards = split_dict_equally(
            {k:v for k,v in bad_data_dict.items() if v['geography'] == "US"}, chunks = n_chunks)
    elif domicile == "nonus":
        data_shards = split_dict_equally(
            {k:v for k,v in bad_data_dict.items() if v['geography'] == "NonUS"}, chunks = n_chunks)

    logger.info("Data shard length for {}, {} = {}".format(asset_class, domicile, np.max([len(x) for x in data_shards])))

    # Save the data chunks
    bad_data_partition_path = "{}/bad_data_partitioned_{}_{}".format(scratch_dir, asset_class, domicile)
    if os.path.exists(bad_data_partition_path):
        shutil.rmtree(bad_data_partition_path)
    os.makedirs(bad_data_partition_path)
    for i, shard in enumerate(data_shards):
        with open("{}/bad_data_partition_{}.pkl".format(bad_data_partition_path, i), "wb") as pfile:
            cloudpickle.dump(shard, pfile)


# Main routine
if __name__ == "__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--numarray", type=int, help="Number of jobs in array", default=100)
    parser.add_argument("-a", "--assetclass", type=str, help="Asset class (bonds|stocks)")
    parser.add_argument("-c", "--cpus", type=str, help="Number of cores")
    parser.add_argument("-d", "--datapath", type=str, help="MNS data path")
    args = parser.parse_args()
    asset_class = args.assetclass
    num_cores = args.cpus
    n_chunks = args.numarray
    mns_data_path = args.datapath
    scratch_dir = os.path.join(mns_data_path, "temp")
    output_dir = os.path.join(mns_data_path, "output")
    assert asset_class in ["bonds", "stocks"], "Invalid asset class (must be in [bonds, stocks])"

    # Set up logging
    logger = logging.getLogger(__name__)
    logfile = '{}/results/logs/{}_Fuzzy_Merge_Train_Linker_{}.log'.format(mns_data_path, getpass.getuser(), asset_class)
    if os.path.exists(logfile):
        os.remove(logfile)
    logger.addHandler(logging.FileHandler(logfile))
    logger.addHandler(logging.StreamHandler(sys.stdout))
    sys.stderr = open(logfile, 'a')
    sys.stdout = open(logfile, 'a')

    # Some initial logging to ensure everything is well-behaved
    logger.info("Running from host {}".format(socket.gethostname()))
    logger.info("OS: {}".format(platform.platform()))
    logger.info("Conda executable: {}".format(sys.executable))
    logger.info("Python version: {}".format(sys.version))
    logger.info("Available cores: {}".format(num_cores))
    logger.info("Asset class: {}".format(asset_class))

    # Read in the data
    geographies = ["US", "NonUS"]
    dfs = {}
    for data_type in ["good_data", "bad_data"]:
        dfs[data_type] = {}
        for geography in geographies:
            logger.info("Loading {}, {}".format(geography, data_type))
            dfs[data_type][geography] = pd.read_stata("{}/fuzzy/{}_{}_{}.dta".format(output_dir, 
                geography, data_type, asset_class))

    # Add geography flags
    for data_type in ["good_data", "bad_data"]:
        for geography in geographies:
            dfs[data_type][geography]["geography"] = geography

    # Concatenate data
    bad_data = pd.concat(list(dfs["bad_data"].values()), axis=0).reset_index().drop("index", axis=1)
    good_data = pd.concat(list(dfs["good_data"].values()), axis=0).reset_index().drop("index", axis=1)

    # Reduce memory footprint
    good_data = good_data.drop(["name_bond_type", "name_bond_legal", 
        "name_firm_categ", "name_jurisdict_categ"], axis=1)
    bad_data = bad_data.drop(["name_bond_type", "name_bond_legal", 
        "name_firm_categ", "name_jurisdict_categ"], axis=1)
    good_data.idusing = good_data.idusing.astype(int)
    bad_data.idmaster = bad_data.idmaster.astype(int)

    # Some info about data size
    logger.info("Good data shape = {}".format(good_data.shape))
    logger.info("Bad data shape = {}".format(bad_data.shape))
    logger.info("Bad data, memory consumption: {} MB".format(sys.getsizeof(bad_data) >> 20))
    logger.info("Good data, memory consumption: {} MB".format(sys.getsizeof(good_data) >> 20))

    # Standardize data format
    good_data = standardize_data_types(good_data)
    bad_data = standardize_data_types(bad_data)

    # Serialize the standardized dataframes
    with open("{}/fuzzy_raw_data_standardized_{}_{}.pkl".format(scratch_dir, asset_class, "all"), "wb") as pfile:
        cloudpickle.dump((bad_data, good_data), pfile)

    # Also store domiciles separately for bonds
    if asset_class == "bonds":
        
        with open("{}/fuzzy_raw_data_standardized_{}_{}.pkl".format(scratch_dir, asset_class, "us"), "wb") as pfile:
            cloudpickle.dump((bad_data[bad_data.geography == "US"], good_data[good_data.geography == "US"]), pfile)
        
        with open("{}/fuzzy_raw_data_standardized_{}_{}.pkl".format(scratch_dir, asset_class, "nonus"), "wb") as pfile:
            cloudpickle.dump((bad_data[bad_data.geography == "NonUS"], good_data[good_data.geography == "NonUS"]), pfile)

    # Set up dictionaries for passing into the linker
    cols_struct = {
        "bonds": ["securityname", "maturitydate", "coupon", "iso_country_code", "currency_id", 
                  "mns_subclass", "extra_security_descriptors", "securityname_raw", "geography"],
        "stocks": ["securityname", "iso_country_code", "currency_id", "mns_subclass", 
                   "extra_security_descriptors", "securityname_raw", "geography"]
    }
    useful_cols = cols_struct[asset_class]
    logger.info("Converting bad data to dictionary of records")
    bad_data_dict = df_to_dict(bad_data[useful_cols])
    logger.info("Converting good data to dictionary of records")
    good_data_dict = df_to_dict(good_data[useful_cols])
    logger.info("Parsing bad data missing fields")
    bad_data_dict = parse_missing_fields(bad_data_dict)
    logger.info("Parsing good data missing fields")
    good_data_dict = parse_missing_fields(good_data_dict)

    # Serialize the dictionaries
    with open("{}/fuzzy_parsed_data_dicts_{}_{}.pkl".format(scratch_dir, asset_class, "all"), "wb") as pfile:
        cloudpickle.dump((bad_data_dict, good_data_dict), pfile)
        
    # Also store separately for bonds
    if asset_class == "bonds":
        
        with open("{}/fuzzy_parsed_data_dicts_{}_{}.pkl".format(scratch_dir, asset_class, "nonus"), "wb") as pfile:
            cloudpickle.dump((
                {k:v for k,v in bad_data_dict.items() if v['geography'] == "NonUS"}, 
                {k:v for k,v in good_data_dict.items() if v['geography'] == "NonUS"}), 
                pfile)

        with open("{}/fuzzy_parsed_data_dicts_{}_{}.pkl".format(scratch_dir, asset_class, "us"), "wb") as pfile:
            cloudpickle.dump((
                {k:v for k,v in bad_data_dict.items() if v['geography'] == "US"}, 
                {k:v for k,v in good_data_dict.items() if v['geography'] == "US"}), 
                pfile)

    # Set up the training file path (this is common to all geographic settings)
    training_file = '{}/raw/fuzzy_merge_training_data/fuzzy_merge_training_data_{}.json'.format(mns_data_path, asset_class)

    # Set up the linkage schema
    common_fields = [
        {'field': 'securityname', 'type': 'String', 'has missing': True},
        {'field': 'iso_country_code', 'type': 'Custom', 'comparator': categorical_comparator, 'has missing': True},
        {'field': 'currency_id', 'type': 'Custom', 'comparator': categorical_comparator, 'has missing': True},
        {'field': 'mns_subclass', 'type': 'Custom', 'comparator': categorical_comparator},
        {'field': 'extra_security_descriptors', 'type': 'Custom', 'comparator': security_descriptor_comparator},
        ]
    bond_only_fields = [
        {'field': 'maturitydate', 'type': 'Custom', 'comparator': maturitydate_comparator, 'has missing': True},
        {'field': 'coupon', 'type': 'Custom', 'comparator': coupon_comparator, 'has missing': True},
        ]

    # Pick appropriate schema
    if asset_class == "bonds":
        fields = common_fields + bond_only_fields
    else:
        fields = common_fields

    # Report the schema
    logger.debug("Linkage Schema")
    logger.debug(fields)

    # Create a new linker object and pass our data model to it
    linker = dedupe.RecordLink(fields, num_cores=num_cores)

    # Set up the regularized logistic regression model
    linker.classifier = RegularizedLogisticRegression()

    # To train the linker, we feed it a sample of records
    random.seed(1); np.random.seed(1)
    linker.sample(bad_data_dict, good_data_dict, 10000)

    # Look for previous training data to bootstrap the linker
    if os.path.exists(training_file):
        logger.info('Reading labeled examples from {}'.format(training_file))
        with open(training_file) as tf:
            training_pairs_prev = read_training(tf)
    else:
        raise Exception("Could not find training data at {}".format(training_file))
    linker.markPairs(training_pairs_prev)

    # Run the training
    linker.train()

    # Determine the optimal threshold
    opt_threshold = find_optimal_threshold(linker, bad_data_dict, good_data_dict)
    linker.opt_threshold = opt_threshold

    # Serialize the linker (note we store separate copies in order to 
    # alleviate I/O pressure when the distributed jobs deserialize it)
    if serialize_linker:
        for domicile in ["all", "us", "nonus"]:
            store_linker(linker, scratch_dir, asset_class, domicile, linker_version)

    # Store sharded bad data
    store_sharded_data(bad_data_dict=bad_data_dict, n_chunks=n_chunks, 
        scratch_dir=scratch_dir, asset_class=asset_class, domicile="all")
    if asset_class == "bonds":
        store_sharded_data(bad_data_dict=bad_data_dict, n_chunks=n_chunks, 
            scratch_dir=scratch_dir, asset_class=asset_class, domicile="us")
        store_sharded_data(bad_data_dict=bad_data_dict, n_chunks=n_chunks, 
            scratch_dir=scratch_dir, asset_class=asset_class, domicile="nonus")

    # Close logs
    sys.stderr.close()
    sys.stdout.close()
    sys.stderr = sys.__stderr__
    sys.stdout = sys.__stdout__
