Replication code for Maggiori, Neiman, and Schreger: "International Currencies and Capital Allocation", forthcoming (2019) in Journal of Political Economy
==============

I. INTRODUCTION
--------------

This README describes the overall structure of the Replication packet for this paper. The code for our project is organized in two steps. First, we have a build, which takes a number of publicly available and privately purchased datasets and combines, cleans, and manipulates them. Second, we have an analysis, where we take the output of these files -- together with additional data -- and generate the tables and figures used in our paper. The uppermost directory of the replication folder therefore has the following six objects:
  i.    	`README.md` (the file you are reading right now)
  ii.   	`README.pdf` (the file you are reading right now)
  iii.  	`MNS_Data_Guide.pdf`
  iv.  		`build` (a folder)
  v.   		`analysis` (a folder)
  vi.   	`raw.zip` (a compressed folder)

As described in the next section of this README, each of the subfolders -- build and analysis -- has its own readme (called `README_BUILD.txt` and `README_ANALYSIS.txt`). For most users, after reading this and the `MNS_Data_Guide.pdf`, the next step is to look at those individual README files.

II. STRUCTURE OF BUILD AND ANALYSIS CODE
--------------

`Master_Build.sh` and `Master_Analysis.sh`, found in the uppermost directories of the build and analysis folders, are scripts that call controller files called `Master_Build_Controller.sh` and `Master_Analysis_Controller.sh`. They do this so that different parameters can be passed to the computing cluster scheduler. For example, in our case, this proved useful as we ran the same code on different research clusters with different numbers of nodes, memories, etc. The controller files then call the `Master_Build.do` and `Master_Analysis.do` files, written in Stata, which implement various steps. `Master_Build.sh` and `Master_Analysis.sh` both use arrays for parallel processing as well as dependencies. This allows, for example, for us to calculate something in parallel using different years from the data before appending the calculations together across all years. In such a case, of course, we need to make sure that the parallel job finishes before running the job that appends their output.   

So, at the highest level, the way to understand the different steps of the overall build and analysis is to start with the `Master_Build.sh` and `Master_Analysis.sh` files. But to better understand some particular analysis, the reader should instead go to `Master_Build.do` or `Master_Analysis.do`, and see from the comments which line calls the program that executes the operation of interest. After that, the reader should go to the actual .do file that is called by `Master_Build.do` or `Master_Analysis.do` to follow the details. For example, if the reader wanted to understand the source of the time-series on currency shares, she might see that on line 71 of `Master_Analysis.do` there is a call to the file `analysis/CurrencyShare_TimeSeries.do` and then would proceed to read that code.   

III. PROPRIETARY DATA
--------------

Most of the data used in this project are proprietary, so cannot be included in this replication packet. The file MNS_Data_Guide.pdf gives an overview of all input files used. In the cases where they are public, the actual files can be found in raw.zip. In several cases where they are provided privately, we provide sample files so the reader can see the formatting, etc., even if they do not actually contain data. Given there are a large number of private data files that are required to run the code, we acknowledge that most readers of this packet will not use the code to literally replicate our results, but rather can look to the code to understand what calculations we made for each result.
