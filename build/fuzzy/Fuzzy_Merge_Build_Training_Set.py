# --------------------------------------------------------------------------------------------------
# Fuzzy_Merge_Build_Training_Set
#
# All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
# Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
# do have an identifier. This allows us to assign a CUSIP to the former records via internal
# cross-linkage.
#
# This file builds the training set for the record-linking logistic regression model
# in a systematic manner, and provides methods for testing the sensitivity of the
# result to the training set construction process.
#
# NOTE: This does not get run as part of the build since it requires human input;
# it is provided here for reference. Its output is used as a raw data source for the
# build process.
# --------------------------------------------------------------------------------------------------
from __future__ import print_function
from joblib import Parallel, delayed

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
import subprocess

from util.serialization import write_training, read_training, \
    clean_list, split_dict_equally, store_linker, parse_missing_fields, df_to_dict, \
    retrieve_linker, _clean_datapoint, _record_str_to_timestamp, _record_timestamp_to_str, \
    write_training

from Fuzzy_Merge_Train_Linker import standardize_data_types, coupon_comparator, maturitydate_comparator, \
    categorical_comparator, subset_training_data

from Fuzzy_Merge_Finalize import concatenate_shards
from rlr import RegularizedLogisticRegression
from tabulate import tabulate

import logging
logger = logging.getLogger(__name__)


#### EXAMPLE INPUT STRUCTURES


# Prior weights (this acts as the prior for the updating process)
prior_betas = np.array([
    -1.0,           # Securityname
    -1.0,           # Country ISO code
    -1.0,           # Currency ID
    -1.0,           # MNS Subclass
    1.0,            # Security Descriptors  
    -0.1,           # Maturitydate (ignored for stocks)
    -1.0,           # Coupon (ignored for stocks)
    1.0,            # Securityname response indicator
    1.0,            # Country ISO code response indicator
    1.0,            # Currency ID response indicator
    1.0,            # Maturitydate response indicator
    1.0             # Coupon response indicator
])

prior_bias = -10

prior_domain = np.array([
    [-np.inf, -.05],            # Securityname
    [-np.inf, -.05],            # Country ISO code
    [-np.inf, -.05],            # Currency ID
    [-np.inf, -.05],            # MNS Subclass
    [.05, np.inf],              # Security Descriptors  
    [-np.inf, -.005],           # Maturitydate (ignored for stocks)
    [-np.inf, -.05],            # Coupon (ignored for stocks)
    [.05, np.inf],              # Securityname response indicator
    [.05, np.inf],              # Country ISO code response indicator
    [.05, np.inf],              # Currency ID response indicator
    [.05, np.inf],              # Maturitydate response indicator
    [.05, np.inf]               # Coupon response indicator
])


# Number of new training samples for iteration
samples_per_iter = {
    'high_prob': 100,
    'mid_prob': 100,
    'low_prob': 100
}


#### TRAINING METHODS


# Function to run one iteration of the training procedure
def do_training_iteration(iteration_number, linkage_fields, bad_data_dict, good_data_dict, asset_class, 
    geo_setting, clip_betas=True, clip_levels=None, prior_weights=None):
    
    # Validate inputs
    if clip_betas and clip_levels is None:
        raise Exception("Please provide clip levels for option clip_betas=True")
    
    # Create a new linker object and pass our data model to it
    linker = dedupe.RecordLink(linkage_fields, num_cores=num_cores)

    # Set up the regularized logistic regression model
    linker.classifier = RegularizedLogisticRegression()
    
    # For the initial iteration, use the prior weights
    if iteration_number == 0:
        assert prior_weights is not None, "Must provide prior weights for initial training iteration"
        prior_intercept, prior_beta = prior_weights
        linker.classifier.weights = prior_beta
        linker.classifier.bias = prior_intercept
        
    # Else we use the latest iteration of the training set
    else:
        
        # To train the linker, we feed it a sample of records
        random.seed(1); np.random.seed(1)
        linker.sample(bad_data_dict, good_data_dict, 10000)

        # Look for the previous training file iteration
        trainset_file_prev_iter = '{}/training_set_build/fuzzy_{}_training_set_v{}.json'.format(
            scratch_dir, asset_class, "_it_{}".format(iteration_number-1)
        )
        assert os.path.exists(trainset_file_prev_iter), "Cannot find previous iteration training set"
        
        # Read the previous training set
        logger.info('Reading labeled examples from {}'.format(trainset_file_prev_iter))
        with open(trainset_file_prev_iter) as tf:
            training_pairs_prev = read_training(tf)
        linker.markPairs(training_pairs_prev)
        
        # Do the training
        logger.info("Training the linker, iteration {}".format(iteration_number))
        linker.train()
        linker.cleanupTraining()
        
        # Apply beta clipping if asked to
        if clip_betas:
            linker.classifier.weights = linker.classifier.weights.clip(
                min=clip_levels[:,0], max=clip_levels[:,1]
            )
        
    # Finally, serialize the linker
    logger.info("Serializing the linker, iteration {}".format(iteration_number))
    store_linker(linker, scratch_dir + "/training_set_build", 
                 asset_class, geo_setting, "_it_{}".format(iteration_number))
    return linker


# Function to run one iteration of the linkage procedure
def do_linker_iteration(iteration_number, code_dir, seed=1, skip_full_pass=False):
    
    # Switch to code directory
    pwd = subprocess.Popen("pwd", shell=True, stdout=subprocess.PIPE).stdout.read().replace("\n", "")
    os.chdir("{}/build/fuzzy".format(code_dir))
    
    # Reporting
    logger.info("Running linker iteration {}".format(iteration_number))
    
    # Sample random shards
    np.random.seed(seed)
    shards = list(np.random.choice(range(500), 30))
    
    # Launch the linkage jobs
    for shard in shards:
        shell_cmd = "./Fuzzy_Merge_Find_Matches.sh -a={} -g={} -c=1 -sd={} -v={} -n={} -s={} -b={} -t={}".format(
                asset_class,
                geo_setting,
                scratch_dir + "/training_set_build",
                "_it_{}".format(iteration_number),
                shard,
                shard - 1,
                (1 if iteration_number == 0 else 0),
                0
            )
        if skip_full_pass:
            shell_cmd += " -f=0"
        logger.info("Shell command: {}".format(shell_cmd))
        logger.info(subprocess.Popen(shell_cmd, shell=True, stdout=subprocess.PIPE).stdout.read())
        
    # Restore previous working dir
    os.chdir(pwd)


# Function for user-assisted record labeling (this requires human interaction)
def run_user_labeling(matches, sampled_cluster_ids):
    labels = {}
    do_break = False
    n_clust = len(sampled_cluster_ids)
    for i, _id in enumerate(sampled_cluster_ids):
        if do_break:
            print("Breaking")
            break
        rec_pair = matches[matches.id_cluster == _id]
        rec_pair.loc[:,'maturitydate'] = rec_pair['maturitydate'].astype(str)
        successful = False
        while not successful:
            print(tabulate(rec_pair.reset_index().drop(['index', 'cusip', 'geography', 'id_cluster', 'provenance', 'match_probability', 'match_round_number'], axis=1), 
                           headers='keys', tablefmt='orgtbl'))
            print("\n")
            user_response = raw_input("{}/{}: Is this pair a match? (y/n/u/q)".format(i, n_clust))
            if user_response == "q":
                do_break = True
                successful = True
            elif user_response == "y":
                labels[_id] = 1
                successful = True
            elif user_response == "n":
                labels[_id] = 0
                successful = True
            elif user_response == "u":
                successful = True
                next
            else:
                pass
    return labels


# Function for machine-assisted record labeling
def run_machine_cooperative_labeling(matches, sampled_cluster_ids, baseline_linker):
    labels = {}
    n_clust = len(sampled_cluster_ids)
    matches = matches[matches.id_cluster.isin(sampled_cluster_ids)]
    matches = matches.drop(['cusip', 'geography', 'provenance', 'match_probability', 'match_round_number'], axis=1)
    for i, _id in enumerate(sampled_cluster_ids):
        try:
            rec_pair = matches[matches.id_cluster == _id]
            prob = baseline_linker.classifier.predict_proba(
                baseline_linker.data_model.distances([[_clean_datapoint(x) for x in rec_pair.to_dict("records")]])
            )
            if prob >= .95:
                labels[_id] = 1
            elif prob <= .5:
                labels[_id] = 0
        except TypeError:
            logger.info("WARNING: Skipping cluster {} due to type error".format(_id))
            logger.info(tabulate(rec_pair))
    return labels


# Function to run one iteration of training set augmentation
def do_training_set_augmentation_iteration(iteration_number, asset_class, geo_setting, samples_per_iter, linkage_fields, seed=1, 
                                           manually_label_high_confidence_pairs=True, label_uncertain_pairs=True, quantile_based_midsampling=True):
    
    # Retrieve the matches
    linker_version = "_it_{}".format(iteration_number)
    match_partition_path = "{}/training_set_build/matches_partitioned_{}_{}_v{}".format(scratch_dir, asset_class, geo_setting, linker_version)
    matches = concatenate_shards(match_partition_path)
    
    # Unpack relevant quantities
    N_high, N_mid, N_low = samples_per_iter['high_prob'], samples_per_iter['mid_prob'], samples_per_iter['low_prob']
    prob_quantiles = matches.quantile([.05, .475, .525 ,.95])['match_probability']
    logger.info("Probability Quantiles:\n" + tabulate(pd.DataFrame(prob_quantiles), tablefmt='orgtbl'))
    
    # Sample cluster IDs for manual labeling
    high_cluster_ids = matches[matches.match_probability >= prob_quantiles[.95]].id_cluster.sample(N_high, random_state=1)
    low_cluster_ids = matches[matches.match_probability <= prob_quantiles[.05]].id_cluster.sample(N_low, random_state=1)
    if quantile_based_midsampling:
        mid_cluster_ids = matches[(matches.match_probability >= prob_quantiles[.475]) & (matches.match_probability <= prob_quantiles[.525])].id_cluster.sample(N_mid, random_state=1)
    else:
        mid_cluster_ids = matches[(matches.match_probability >= .475) & (matches.match_probability <= .525)].id_cluster.sample(N_mid, random_state=1)
        
    # Run user labeling
    if True:
        logger.info("Running user labeling for high-probability matches")
        high_labels = run_user_labeling(matches, high_cluster_ids)
    else:
        logger.info("ATTENTION: Assigning label 1 to all high-probability matches without manual review")
        high_labels = {_id: 1 for _id in high_cluster_ids}

    if manually_label_high_confidence_pairs:
        logger.info("Running user labeling for low-probability matches")
        low_labels = run_user_labeling(matches, low_cluster_ids)
    else:
        logger.info("ATTENTION: Assigning label 0 to all low-probability matches without manual review")
        low_labels = {_id: 0 for _id in low_cluster_ids}

    if label_uncertain_pairs:
        logger.info("Running user labeling for mid-probability matches")
        mid_labels = run_user_labeling(matches, mid_cluster_ids)

    # Merge labels
    all_labels = high_labels.copy()
    all_labels.update(low_labels)
    if label_uncertain_pairs:
        all_labels.update(mid_labels)
    negatives = [k for k,v in all_labels.items() if v == 0]
    positives = [k for k,v in all_labels.items() if v == 1]
 
    # For the first iteration, start a training set from scratch
    if iteration_number == 0:
        training_set = {'distinct': [], 'match': []}
    else:
        trainset_file_prev_iter = '{}/training_set_build/fuzzy_{}_training_set_v{}.json'.format(
            scratch_dir, asset_class, "_it_{}".format(iteration_number-1)
        )
        if os.path.exists(trainset_file_prev_iter):
            print('Reading labeled examples from', training_file)
            with open(trainset_file_prev_iter) as tf:
                training_set = read_training(tf)
        else:
            raise Exception("Could not find prevous iteration training set at iteration {}".format(iteration_number))
            
    # Insert new points into the training set
    fields = [x['field'] for x in linkage_fields]
    for _id in positives:
        record_pair = matches[matches.id_cluster == _id][fields].to_dict('records')
        training_set['match'].append((_clean_datapoint(record_pair[0]), _clean_datapoint(record_pair[1])))
    for _id in negatives:
        record_pair = matches[matches.id_cluster == _id][fields].to_dict('records')
        training_set['distinct'].append((_clean_datapoint(record_pair[0]), _clean_datapoint(record_pair[1])))
    
    # Write out to disk
    trainset_file_current_iter = '{}/training_set_build/fuzzy_{}_training_set_v{}.json'.format(scratch_dir, asset_class, linker_version)
    logger.info("Writing current training set iteration to {}".format(trainset_file_current_iter))
    with open(trainset_file_current_iter, 'w') as tf:
        write_training(training_set, tf)
    
    return training_set


# Function to archive build round
def archive_build_results(build_name):
    
    # Switch to tmp data directory
    pwd = subprocess.Popen("pwd", shell=True, stdout=subprocess.PIPE).stdout.read().replace("\n", "")
    os.chdir("{}/training_set_build".format(scratch_dir))

    # Archive the build
    arch_dir = "archived_build_{}".format(build_name)
    logger.info("Archiving training set build to {}".format(arch_dir))
    os.mkdir(arch_dir)
    os.system("mv fuzzy_{}_training_set_* {}".format(asset_class, arch_dir))
    os.system("mv matches_partitioned_{}_{}_* {}".format(asset_class, geo_setting, arch_dir))
    os.system("mv serialized_linker_{}_{}_* {}".format(asset_class, geo_setting, arch_dir))
    
    # Restore previous working dir
    os.chdir(pwd)

