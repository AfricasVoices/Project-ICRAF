# Project ICRAF
A Data pipeline for the ICRAF project.

This pipeline fetches all project data from a Rapid Pro instance, and processes it to produce CSV files suitable
for downstream analysis.

## Pre-requisites
Before the pipeline can be run, the following tools must be installed:
 - Docker
 - Bash
 
Development requires the following additional tools:
 - Python 3.6+
 - pipenv
 - git

## Usage
Running the pipeline requires (1) creating a phone number <-> UUID table to support de-identification of 
respondents, (2) fetching all the relevant data from Rapid Pro, and (3) processing the raw data to
produce the outputs required for coding and then for analysis. 

To simplify the configuration and execution of these stages, this project includes a `run_scripts`
directory, which contains shell scripts for driving each of the stages. 
More detailed descriptions of the functions of each of those stages, and instructions for using
the run scripts, are provided below. 

### 1. Phone Number <-> UUID Table
First, create an empty phone number <-> UUID table by running the following command in 
the `run_scripts` directory:

```
$ ./1_create_uuid_table.sh <data-root> 
```

where `data-root` is an absolute path to the directory in which all pipeline data should be stored. 
The UUID table will be saved to a file in the directory `<data-root>/UUIDs`.

### 2. Fetch Raw Data
Next, fetch all the raw data required by the pipeline from Rapid Pro by running the following command in 
the `run_scripts` directory:

`pipenv run python 2_fetch_raw_data.py <user> <avf-credentials-service-account-file-path> <pipeline-config-file-path> <rapid-pro-root> <data-root>`.

where:
 - `user` is the identifier of the person running the script, for use in the TracedData Metadata 
   e.g. `user@africasvoices.org` 
- `avf-credentials-service-account-file-path` is an absolute path to a json credentials file 
    that grants one access to download textit token.
- `pipeline-config-file-path ` is an absolute path to a json file containing textit url and
   a google cloud link to a textit api key file.
- `rapid-pro-root` is an absolute path to the directory to store a local clone of 
   [RapidProTools](https://github.com/AfricasVoices/RapidProTools) in.
   The RapidProTools project hosts the re-usable RapidPro data fetchers.
   The exact version required by this project is checked out automatically.
 - `data-root` is an absolute path to the directory in which all pipeline data should be stored.
   Raw data will be saved to TracedData JSON files in `<data-root>/Raw Data`.

 