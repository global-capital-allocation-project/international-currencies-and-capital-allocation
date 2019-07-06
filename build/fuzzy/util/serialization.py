# --------------------------------------------------------------------------------------------------
# Serialization
#
# All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
# Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
# do have an identifier. This allows us to assign a CUSIP to the former records via internal
# cross-linkage.
#
# This file provides serialization utilities for the fuzzy merge code.
# --------------------------------------------------------------------------------------------------
import os
import pandas as pd
import numpy as np
import tenacity
import cloudpickle
import simplejson as json
import dedupe.serializer as serializer


# Deserialization function that is robust to IOErrors, for concurrent reads
@tenacity.retry(wait=tenacity.wait_fixed(2) + tenacity.wait_random_exponential(multiplier=1, max=10),
                retry=tenacity.retry_if_exception_type(IOError),
                stop=tenacity.stop_after_attempt(25))
def _safe_deserialize(filename, mode="rb"):
    with open(filename, mode) as pfile:
        return cloudpickle.load(pfile)


def safe_deserialize(filename, mode="rb"):
    try:
        return _safe_deserialize(filename, mode)
    except tenacity.RetryError:
        raise Exception("Process {} timed out while waiting to acquire read access to file {}".format(os.getpid(), filename))


# Utility to retrieve serialized linker
def retrieve_linker(scratch_dir, asset_class, geo_setting, linker_version):
    target_file = "{}/serialized_linker_{}_{}_v{}.pkl".format(scratch_dir, asset_class, geo_setting, linker_version)
    (linker, weights, bias) = safe_deserialize(target_file)
    linker.classifier.weights = weights
    linker.classifier.bias = bias
    return linker


# Utility function to convert records between timestamp and str formats
def _record_timestamp_to_str(record):
    return {k: (v if type(v) != pd.Timestamp else v.strftime("%Y-%m-%d"))
        for k, v in record.items()}


# Same thing, in reverse
def _record_str_to_timestamp(record):
    return {k: (pd.to_datetime(v) if type(v) in [str, unicode]
        and k == "maturitydate" else v) for k, v in record.items()}


# Custom function to write out the training data
def write_training(training_obj, file_obj):
    """
    Write to a json file that contains labeled examples;
    handles datetime serialization correctly
    """
    if type(training_obj) == dict:
        training_pairs_raw = training_obj
    elif hasattr(training_obj, "training_pairs"):
        training_pairs_raw = training_obj.training_pairs
    else:
        raise Exception("Training object not recognized in write_training")
    # Clean the data to make it serializable
    training_pairs_cleaned = {}
    for pair_type in ["distinct", "match"]:
        training_pairs_cleaned[pair_type] = []
        pair_list = training_pairs_raw[pair_type]
        for (x, y) in pair_list:
            clean_pair = (_record_timestamp_to_str(x), _record_timestamp_to_str(y))
            training_pairs_cleaned[pair_type].append(clean_pair)

    # Serialize the data to JSON
    json.dump(training_pairs_cleaned,
              file_obj,
              default=serializer._to_json,
              tuple_as_array=False,
              ensure_ascii=True)


# Datapoint cleaning for training reader
def _clean_datapoint(datapoint):
    return {k: v if v not in [pd.NaT, '', np.nan, 'nan'] and not pd.isna(v) 
            else None for k,v in datapoint.items()}


# Custom training reader
def read_training(training_file):
    """
    Read training from previously built training data file object;
    handles datetime serialization
    """
    training_pairs = json.load(training_file,
                               cls=serializer.dedupe_decoder)
    training_pairs_cleaned = {}
    for pair_type in ["distinct", "match"]:
        training_pairs_cleaned[pair_type] = []
        pair_list = training_pairs[pair_type]
        for (x, y) in pair_list:
            x_clean = _clean_datapoint(_record_str_to_timestamp(x))
            y_clean = _clean_datapoint(_record_str_to_timestamp(y))
            clean_pair = (x_clean, y_clean)
            training_pairs_cleaned[pair_type].append(clean_pair)
    return training_pairs_cleaned


# Small utility to purge lists of nan values
def clean_list(in_set):
    return list({x for x in in_set if x == x})


# Function to shard the data
def split_dict_equally(input_dict, chunks=2):
    """
    Splits dict by keys. Returns a list of dictionaries.
    """
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in input_dict.iteritems():
        return_list[idx][k] = v
        if idx < chunks-1:
            idx += 1
        else:
            idx = 0
    return return_list


# Utility to convert missing fields to None for linker
def parse_missing_fields(data):
    """
    This uses nested dictionary comprehensions; it's efficient but perhaps not too
    readable. In essence, it just converts blank strings and null types to None types 
    """
    def _get_nan_synonims(k):
        if k == "extra_security_descriptors":
            return [pd.NaT, np.nan]
        else:
            return [pd.NaT, '', np.nan]

    data = {id_no: {k: (v if v not in _get_nan_synonims(k) and not pd.isna(v) 
                else None) for k,v in datapoint.items()} for id_no, datapoint in data.items()}
    return data


# Conversion function (df -> dictionary of records)
def df_to_dict(df):
    records = df.to_dict(orient="records")
    indices = df.index
    return {ix: record for ix, record in zip(indices, records)}


# Utility function to serialize the linker
def store_linker(linker, scratch_dir, asset_class, geo_setting, linker_version):
    linker.active_learner = None
    with open("{}/serialized_linker_{}_{}_v{}.pkl".format(scratch_dir, asset_class, geo_setting, linker_version), "wb") as pfile:
        cloudpickle.dump((linker, linker.classifier.weights, linker.classifier.bias), pfile)


# Function to get # of active jobs
def get_active_jobs_num():
    return len(subprocess.Popen("squeue", shell=True, stdout=subprocess.PIPE).stdout.read().split("\n")) - 2


def get_list_chunks(l, n):
    return [l[j::n] for j in range(n)]


def get_list_chunks_fixedsize(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in xrange(0, len(l), n))


