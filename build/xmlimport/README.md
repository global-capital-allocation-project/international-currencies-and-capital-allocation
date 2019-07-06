### Readme

This folder is for scripts use to take the raw data obtained from the data providers 
and 'massage' it into a form that can be used by the main scripts.

At this time, it involves this process:

- From the commercial providers, data is placed into folders as desired
- A bash script is run to process the folder as a job array under SLURM
- The SLURM script determines which Job Array slot it is and then picks the appropriate input archive file (position as ordered alphabetically, with 0-based index), and calls the main script with turns the archive into SAS-exported data
maggiori_lab/MNS
To run this main job-array script, execute:

```bash
./process_directory_slurm_array.sh directory_to_process
```

The directory_to_process can be a relative path, full path, or even '.' or '..'.
The full path is determined by this bash script before passing off things to the SLURM script.
This script also creates as `slurm_logs/` folder inside `directory_to_process`, where the SLURM
job STDOUT and STDERR files are placed.

If any of the array jobs fail under SLURM, you can re-run parts of this script manually, though
the `--array=` parameter to `sbatch` should be a comma-separated list of the job slots to re-run.


*Archive processing*<br>
An `xml/` folder is created inside `directory_to_process` and the concatenated XML file is placed here.
When the SAS processing is done, this file is compressed into a tar/gzipped archive and removed.

During the archive expansion, the XML files are temporarily placed in a uniquely-named folder
on the compute node at `/tmp/`, so that side-by-side runs on the same compute node will not step
on each other's toes. Also, this assumes that each expanded archive will be 4 GB in size or less
(max `/tmp/` size total is 250 GB).

*SAS processing*<br>
Currently a `temp/` folder is used for SAS' WORK environment.
Log and final output files are placed in a `sas/` folder within `directory_to_process`.

*Technical notes*<br>
Please note that the scripts in this folder are built to be executed on a SLURM cluster computing environment,
such as the Odyssey research cluster at Harvard. The build may need to be adjusted in order to work in different 
environments.

Prior to running the scripts in these folders, please be sure to fill in the following parameters in the files:
      <CODE_PATH>: Path to the build code on the host system
      <DATA_PATH>: Path to the folder containing the data, in which the build is executed
      <SLURM_PARTITION>: The name of the SLURM partition on which the jobs are to be executed

The module calls (module load [...]) correspond to those used on the SLURM-based system on the 
Odyssey research cluster at Harvard. These will likely have to be adapted when running on
different systems.
