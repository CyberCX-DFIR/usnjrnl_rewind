# Usnjrnl Rewind / USN full path builder

For an explanation of Usnjrnl Rewind, read the blog post [here](https://cybercx.com.au/blog/nfts-usnjrnl-rewind/)).

This script will process the outputs of Eric Zimmerman's MFTEcmd tool and produce a csv that has the complete and correct path for every file and folder (no more Unknowns). 

It uses a different method to compute the full paths, essentially by _rewinding_ the journal and keeping track of all  changes as they occur going from the last entry in the journal to the first. 

_If your journal has no gaps (under normal circumstances there should not be any), then there should be no unknown paths in the output._

### Usage
```
$ python3 usnjrnl_rewind.py –h

usage: usnjrnl_rewind.py [-h] [-m MFT_PROCESSED_CSV_FILE] [-u USNJRNL_PROCESSED_CSV_FILE] output_path

USN full path builder v0.4

positional arguments:
  output_path           Output folder path (will create if non-existent)

optional arguments:
  -h, --help            show this help message and exit
  -m MFT_PROCESSED_CSV_FILE, --mft_processed_csv_file MFT_PROCESSED_CSV_FILE
                        processed $MFT csv from MFTECMD (required)
  -u USNJRNL_PROCESSED_CSV_FILE, --usnjrnl_processed_csv_file USNJRNL_PROCESSED_CSV_FILE
                        processed $Usnjrnl:$J csv from MFTECMD (required)

(c) 2024 Yogesh Khatri, CyberCX

This tool needs the output of Mftecmd for both USN and MFT 
(no need to process both together when processing the USN in mftecmd)
```

### Sample output
```
% python3 usnjrnl_rewind.py -m mftv3.csv -u usnv3.csv rewind_out
[.] Creating an SQLite database here: ./rewind_out/NTFS.sqlite
[.] Adding MFT data to database..
[.] Adding USNJRNL:$J data to database..
[.] Database creation time: 00:00:05
[.] ..Rewinding journal and computing the full paths now..
[.] Created the USNJRNL full path csv here: ./rewind_out/USNJRNL.fullPaths.csv
[.] Adding full path data to database..
[.] Finished in total time: 00:00:08
```
