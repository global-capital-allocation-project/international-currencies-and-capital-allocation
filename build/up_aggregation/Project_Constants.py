# --------------------------------------------------------------------------------------------------
# Ultimate parent aggregation analysis: Project constants
#
# The files in this folder provide an implementation of the ultimate parent aggregation algorithm of 
# Coppola, Maggiori, Neiman, and Schreger (2019). For a detailed discussion of the aggregation algorithm, 
# please refer to that paper.
#
# This file defines constants that are used by the code in UP_Aggregation.py.
# 
# Technical notes:
#   - Prior to running the build, please be sure to fill in the following parameter in the script:
#           <DATA_PATH>: Path to the folder containing the data, in which the build is executed
# --------------------------------------------------------------------------------------------------
import socket
import getpass

# Host and username
host = socket.gethostname()
user = getpass.getuser()

# Path to data folder
mns_data = "<DATA_PATH>"
project_dir = "{}/output/up_analysis/results/".format(mns_data)

# List of countries classified as tax havens
tax_havens = 	["BHS", "CYM", "COK", "DMA", "LIE", "MHL", "NRU", "NIU", "PAN", "KNA", 
				 "VCT", "BMU", "CUW", "JEY", "BRB", "MUS", "VGB", "VIR", "ATG", "AND", 
				 "AIA", "ABW", "BLZ", "BRN", "CPV", "GIB", "GRD", "DOM", "GGY", "IMN",
				 "MCO", "MSR", "PLW", "VCT", "WSM", "SMR", "SYC", "MAF", "TTO", "TCA", 
				 "VUT", "ANT", "CHI", "GLP", "IMY", "MTQ", "REU", "SHN", "TUV", "WLF", 
				 "HKG"]

# Hardcoded source preference order (lower number = higher priority)
source_preference_order = {'bvd': 1, 'dlg': 2, 'fds': 3, 'ciq': 4, 'sdc': 5}
