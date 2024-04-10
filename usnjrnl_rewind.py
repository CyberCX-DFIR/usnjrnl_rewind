"""
(c) 2024 Yogesh Khatri (@Swiftforensics), CyberCX

Script to process USNJRNL data output from MFTEcmd 
and add the correct full path information by 
rewinding the journal entries one by one. 

By doing so, this provides the correct paths for
files and folders as they existed when the journal
event occurred.

Author  : Yogesh Khatri
License : MIT

"""
import argparse
import csv
import os
import random
import sqlite3
import time

version = 0.4

from csv_to_sqlite import import_csv, sanitize_remove_nulls
from string import ascii_uppercase

def get_time_taken_string(start_time, end_time):
    time_taken = end_time - start_time
    try:
        run_time_HMS = time.strftime('%H:%M:%S', time.gmtime(time_taken))
    except (OSError, ValueError) as ex:
        print('[!] Failed to calc time string',str(ex))
        run_time_HMS = f'-failed-to-calc , time_taken={time_taken}'
    return run_time_HMS

def add_to_sqlite(path, sqlite_db_path, table_name, perform_cleaning=True):
    temp_folder = os.path.split(os.path.realpath(path))[0]
    cleaned_csv_path = sanitize_remove_nulls(path, temp_folder) if perform_cleaning else path
    ret = import_csv(cleaned_csv_path, sqlite_db_path, table_name, guess_column_types=True)
    if cleaned_csv_path != path:
        try:
            os.remove(cleaned_csv_path)
        except OSError as ex:
            print(f'[!] Failed to remove temp file : {cleaned_csv_path} Error was:', str(ex))
    return ret

def create_sqlitedb(output_path, mft_csv_path, usnjrnl_csv_path):
    '''Create db and return path if successful, else return empty string'''
    start_time = time.time()
    sqlite_path = os.path.join(output_path, 'NTFS.sqlite')
    if os.path.exists(sqlite_path):
        rand = ''.join(random.choice(ascii_uppercase) for i in range(4))
        sqlite_path = os.path.join(output_path, f'NTFS_{rand}.sqlite')

    print(f'[.] Creating an SQLite database here: {sqlite_path}')
    
    print('[.] Adding MFT data to database..')
    if not add_to_sqlite(mft_csv_path, sqlite_path, 'MFT'):
        print('[!] Failed to add to sqlite.')
        return ''

    print('[.] Adding USNJRNL:$J data to database..')
    if not add_to_sqlite(usnjrnl_csv_path, sqlite_path, 'USNJRNL'):
        print('[!] Failed to add to sqlite.')
        return ''

    end_time = time.time()
    print(f'[.] Database creation time: {get_time_taken_string(start_time, end_time)}')
    return sqlite_path

def rewind(output_path, mft_csv_path, usnjrnl_csv_path):
    start_time = time.time()
    sqlite_path = create_sqlitedb(output_path, mft_csv_path, usnjrnl_csv_path)
    if not sqlite_path:
        return
    out_csv_path = os.path.join(output_path, 'USNJRNL.fullPaths.csv')
    print('[.] ..Rewinding journal and computing the full paths now..')
    create_journal_rewind_csv(sqlite_path, out_csv_path, 'MFT', 'USNJRNL')
    print(f'[.] Created the USNJRNL full path csv here: {out_csv_path}')
    print(f'[.] Adding full path data to database..')
    if not add_to_sqlite(out_csv_path, sqlite_path, 'USNJRNL_FullPaths', perform_cleaning=False):
        print('[!] Failed to add csv to sqlite.')

    end_time = time.time()
    
    print(f'[.] Finished in total time: {get_time_taken_string(start_time, end_time)}')

def get_full_path(entry, lookup_dict, path):
    parent_path = "<UNKNOWN>"
    if entry in lookup_dict:
        file_name, parent_entry, parent_name = lookup_dict[entry]
        if parent_entry == "5-5":
            parent_path = "."
        else:
            parent_path = get_full_path(parent_entry, lookup_dict, parent_name)
    if path:
        return parent_path + '\\' + path
    return parent_path

def create_journal_rewind_csv(sqlite_db_path, out_csv_path, mft_table_name, usn_table_name):
    
    try:
        db = sqlite3.connect(sqlite_db_path)
        db.row_factory = sqlite3.Row
    except:
        print(f"[!] Failed to open db at {sqlite_db_path}")
        return False
    
    # The below query will convert all -ve ParentSequenceNumbers to +ve
    # This occurs due to a bug in older MFTEcmd (fixed on 9 Mar 2024).
    query_bugfix = '''
        UPDATE {MFT_TABLE} SET ParentSequenceNumber=ParentSequenceNumber&65535
        where ParentSequenceNumber < 0
    '''
    try:
        mft_query = query_bugfix.format(MFT_TABLE=mft_table_name)
        results = db.execute(mft_query)
        if results.rowcount:
            print(f'[.] Buggy values fixed for {results.rowcount} rows')
        db.commit()
    except sqlite3.Error as ex:
        print(f"[!] Failed query. Exception was " + str(ex))
        print(f"[!] Query was {mft_query}")
        db.close()
        return False
    
    query = '''
        SELECT m1.EntryNumber || '-' || m1.SequenceNumber as Entry, m1.FileName,
        m1.ParentEntryNumber || '-' || m1.ParentSequenceNumber as ParentEntry, 
        ifnull(m2.FileName, '') as ParentName
        FROM {MFT_TABLE} m1 LEFT JOIN {MFT_TABLE} m2 
        ON m1.ParentEntryNumber = m2.EntryNumber AND m1.ParentSequenceNumber = m2.SequenceNumber 
        WHERE m1.InUse = "True"
        UNION ALL
        SELECT m1.EntryNumber || '-' || (m1.SequenceNumber - 1) as Entry, m1.FileName,
        m1.ParentEntryNumber || '-' || m1.ParentSequenceNumber as ParentEntry, 
        ifnull(m2.FileName, '') as ParentName
        FROM {MFT_TABLE} m1 LEFT JOIN {MFT_TABLE} m2 
        ON m1.ParentEntryNumber = m2.EntryNumber AND m1.ParentSequenceNumber = m2.SequenceNumber 
        WHERE m1.InUse = "False" and m2.Inuse = "True"
		UNION ALL
		SELECT m1.EntryNumber || '-' || (m1.SequenceNumber - 1) as Entry, m1.FileName,
        m1.ParentEntryNumber || '-' || m1.ParentSequenceNumber as ParentEntry, 
        ifnull(m2.FileName, '') as ParentName
        FROM {MFT_TABLE} m1 LEFT JOIN {MFT_TABLE} m2 
        ON m1.ParentEntryNumber = m2.EntryNumber AND m1.ParentSequenceNumber = (m2.SequenceNumber  - 1)
        WHERE m1.InUse = "False"  and m2.Inuse = "False"
    '''
    # The above query produces multiple output for entry when ADS are encountered:
    # EG:   10-10	$UpCase	        5-5	    .
    #       10-10	$UpCase:$Info	5-5	    .
    try:
        mft_query = query.format(MFT_TABLE=mft_table_name)
        results = db.execute(mft_query)
    except sqlite3.Error as ex:
        print(f"[!] Failed query. Exception was " + str(ex))
        print(f"[!] Query was {mft_query}")
        db.close()
        return False
    
    parent_lookup = {} # { Entry : (EntryName, ParentEntry, ParentName), .. }

    for result in results:
        parent_lookup[result['Entry']] = (result['FileName'], result['ParentEntry'], result['ParentName'])

    query = '''
        SELECT EntryNumber, ParentEntryNumber, Name, ParentPath,
        UpdateReasons, UpdateTimestamp, ParentSequenceNumber, SequenceNumber, 
        FileAttributes, Extension, UpdateSequenceNumber, OffsetToData, SourceFile,
        EntryNumber||'-'||SequenceNumber as Entry, 
        ParentEntryNumber||'-'||ParentSequenceNumber as ParentEntry
        FROM {USNJRNL_TABLE} 
        ORDER BY UpdateTimestamp DESC, UpdateSequenceNumber DESC
    '''

    field_names = ('Name', 'Extension', 'EntryNumber', 'SequenceNumber', 'ParentEntryNumber', 
                   'ParentSequenceNumber', 'ParentPath', 'UpdateSequenceNumber', 'UpdateTimestamp',
                   'UpdateReasons', 'FileAttributes', 'OffsetToData', 'SourceFile')
    with open(out_csv_path, 'w', encoding='utf8', newline='', buffering=50000) as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names)
        writer.writeheader()
        items_to_write = []
    
        try:
            db = sqlite3.connect(sqlite_db_path)
            db.row_factory = sqlite3.Row
        except:
            print(f"[!] Failed to open db at {sqlite_db_path}")
            return False
        try:
            usn_query = query.format(USNJRNL_TABLE=usn_table_name, MFT_TABLE=mft_table_name)
            results = db.execute(usn_query)
        except sqlite3.Error as ex:
            print(f"[!] Failed query. Exception was " + str(ex))
            print(f"[!] Query was {usn_query}")
            db.close()
            return False

        last_item = {}
        for result in results:
            name = result['Name']
            entry_num = int(result['EntryNumber'])
            parent_entry_num = int(result['ParentEntryNumber'])
            ts = result['UpdateTimestamp']
            reasons = result['UpdateReasons']
            attributes = result['FileAttributes']
            extension = result['Extension']
            seq_num = result['SequenceNumber']
            parent_seq_num = result['ParentSequenceNumber']
            update_seq_number = result['UpdateSequenceNumber']
            off_to_data = result['OffsetToData']
            source_file = result['SourceFile']

            entry = result['Entry']
            parent_entry = result['ParentEntry']
            path_changed = False

            if "RenameOldName" in reasons:
                # Replace entry in lookup dict, need parent name for this
                parent_name = parent_lookup.get(parent_entry, ('','',''))[0]
                # Replace with new parent entry & parent name
                parent_lookup[entry] = name, parent_entry, parent_name
                path_changed = True

            elif "FileDelete" in reasons:
                # Check if it currently exits. If not, add to parent_lookup
                if entry not in parent_lookup:
                    # try to lookup parent name
                    p_name = parent_lookup.get(parent_entry, ('','',''))[0]
                    parent_lookup[entry] = (name, parent_entry, p_name)

            if parent_entry == "5-5":
                path_prefix = '.' # nothing to do

            # see if prev computed full path
            elif last_item and path_changed == False and \
                    last_item['timestamp'] == ts and \
                    last_item['entry'] == entry and \
                    last_item['parent_entry'] == parent_entry:
                path_prefix = last_item['path_prefix']
            
            elif parent_entry in parent_lookup:
                parent_file_name, _, _ = parent_lookup[parent_entry]
                path_prefix = get_full_path(parent_entry, parent_lookup, parent_file_name)
            else:
                # unknown
                path_prefix = "<UNKNOWN>"
                print(f'[!] Error: Encountered an UNKNOWN path, report this to the developer! Item update_seq_number={update_seq_number}')

            #fullpath = path_prefix + '\\' + name

            item = {'Name': name,
                    'Extension': extension,
                    'EntryNumber': entry_num,
                    'SequenceNumber': seq_num, 
                    'ParentEntryNumber': parent_entry_num, 
                    'ParentSequenceNumber': parent_seq_num,
                    'ParentPath': path_prefix,
                    'UpdateSequenceNumber': update_seq_number,
                    'UpdateTimestamp': ts,
                    'UpdateReasons': reasons,
                    'FileAttributes': attributes,
                    'OffsetToData': off_to_data,
                    'SourceFile': source_file
                    }
            items_to_write.append(item)

            last_item['entry'] = entry
            last_item['parent_entry'] = parent_entry
            last_item['timestamp'] = ts
            last_item['path_prefix'] = path_prefix

            if len(items_to_write) >= 50000:
                writer.writerows(items_to_write)
                items_to_write = []
        
        if items_to_write:
            writer.writerows(items_to_write)

        db.close()
        return True

def main():
    usage = '''(c) 2024 Yogesh Khatri, CyberCX. \n\n
This tool needs the output of Mftecmd for both USN and MFT 
(no need to process both together when processing the USN in mftecmd)\n '''
    parser = argparse.ArgumentParser(description=f'USN full path builder v{version}', epilog=usage, 
                formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-m', '--mft_processed_csv_file', help='processed $MFT csv from MFTECMD (required)', required=True)
    parser.add_argument('-u', '--usnjrnl_processed_csv_file', help='processed $Usnjrnl:$J csv from MFTECMD (required)', required=True)
    parser.add_argument('output_path', help='Output folder path (will create if non-existent)')
    args = parser.parse_args()

    output_path = args.output_path
    usnjrnl_csv_path = args.usnjrnl_processed_csv_file
    mft_csv_path = args.mft_processed_csv_file

    if not os.path.exists(mft_csv_path):
        print('[!] Error: Need to specify processed $MFT\'s csv path to proceed')
        return
    if not os.path.exists(usnjrnl_csv_path):
        print('[!] Error: Need to specify processed $Usnjrnl:$J\'s csv path to proceed')
        return
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    rewind(output_path, mft_csv_path, usnjrnl_csv_path)
        
if __name__ == "__main__":
    main()