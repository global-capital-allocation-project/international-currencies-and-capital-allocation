# --------------------------------------------------------------------------------------------------
# CMNS ultimate parent aggregation: Manual corrections for data errors
#
# The files in this folder provide an implementation of the ultimate parent aggregation algorithm of 
# Coppola, Maggiori, Neiman, and Schreger (2019). For a detailed discussion of the aggregation algorithm, 
# please refer to that paper.
#
# This file implements a number of manual correction to the raw data, which address mistaken reporting.
# Note that these corrections are not meant to be comprehensive, but rather reflect the errors that
# we became aware of in the course of the project.
# --------------------------------------------------------------------------------------------------

# CUSIP6 codes to be dropped from source
drop_cusip6 = {
	'ciq': [
		'92857W'	# Reference to outdated Vodafone-Thales link
		'45579E',	# Bad info for Indivior
		'G4766E'	# Bad info for Indivior
	],
	'sdc': [],
	'bvd': [],
	'dlg': [],
	'fds': []
}

# Extra manual links (format child: parent)
extra_link = {
	'69384E': '172967',  # Citigroup vehicles
	'251525': '25155M',  # Deutsche Bank vehicles
	'D218FW': '25155M',	 # Deutsche Bank vehicles
	'F1R1JQ': '05565A',	 # BNP Paribas vehicles
	'86060R': 'B639CJ',	 # Anheuser-Busch Inbev
	'29890L': '29874Q',  # European Bank Reconstruction & Dev
	'N4S6AW': '46625H',	 # JP Morgan vehicles
	'46623C': '46625H',  # JP Morgan vehicles
	'466247': '46625H',  # JP Morgan vehicles
	'46625M': '46625H',  # JP Morgan vehicles
	'46625S': '46625H',  # JP Morgan vehicles
	'46627B': '46625H',  # JP Morgan vehicles
	'466284': '46625H',  # JP Morgan vehicles
	'46628A': '46625H',  # JP Morgan vehicles
	'46628C': '46625H',  # JP Morgan vehicles
	'46628K': '46625H',  # JP Morgan vehicles
	'46628M': '46625H',  # JP Morgan vehicles
	'46628R': '46625H',  # JP Morgan vehicles
	'46628T': '46625H',  # JP Morgan vehicles
	'46628V': '46625H',  # JP Morgan vehicles
	'46629A': '46625H',  # JP Morgan vehicles
	'46629B': '46625H',  # JP Morgan vehicles
	'46629K': '46625H',  # JP Morgan vehicles
	'46629N': '46625H',  # JP Morgan vehicles
	'46629T': '46625H',  # JP Morgan vehicles
	'46629V': '46625H',  # JP Morgan vehicles
	'46635J': '46625H',  # JP Morgan vehicles
	'46635N': '46625H',  # JP Morgan vehicles
	'46640B': '46625H',  # JP Morgan vehicles
	'46640M': '46625H',  # JP Morgan vehicles
	'46641C': '46625H',  # JP Morgan vehicles
	'46641X': '46625H',  # JP Morgan vehicles
	'46643B': '46625H',  # JP Morgan vehicles
	'46643H': '46625H',  # JP Morgan vehicles
	'46643K': '46625H',  # JP Morgan vehicles
	'46643Q': '46625H',  # JP Morgan vehicles
	'46643V': '46625H',  # JP Morgan vehicles
	'46646T': '46625H',  # JP Morgan vehicles
	'G5201J': '46625H',  # JP Morgan vehicles
	'L5781M': '46625H',  # JP Morgan vehicles
	'N4S6AW': '46625H',  # JP Morgan vehicles
	'P1R6BE': '46625H',  # JP Morgan vehicles
	'U4806W': '46625H',  # JP Morgan vehicles
	'U48133': '46625H',  # JP Morgan vehicles
	'U5251F': '46625H',  # JP Morgan vehicles
	'89114M': '891160',	 # Toronto Dominion Bank
	'89120D': '891160',  # Toronto Dominion Bank
	'06371E': '063671',	 # Bank of Montreal
	'78012Y': '780087',  # RB Canada
}
