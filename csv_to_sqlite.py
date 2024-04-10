
"""
(c) 2021 Yogesh Khatri

Script to convert a csv file to an sqlite database. 
By default, it will delete an older db if it exists!
By default, all fields(columns) are set to TEXT, 
unless you use the -g option to guess based on first
row data.

Author  : Yogesh Khatri, yogesh@swiftforensics.com
License : MIT

TODO:
 - Add date conversion to YYYY-MM-DD HH:MM:SS format 
   for DD/MM/YYYY dates and MM/DD/YYYY (US format)
"""
import argparse
import csv
import mmap
import os
import re
import sqlite3

def table_exists(db_conn, table_name):
    ''' Checks if a table with specified name exists in an sqlite db.
        Will throw an exception on error.
    '''
    #try:
    cursor = db_conn.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}"')
    for row in cursor:
        return True
    #except sqlite3.Error as ex:
    #    print ("Failed to list tables of db when checking for '{}'. Error Details:{}".format(table_name, str(ex)) )
    return False

def create_table(db, table_name, columns_info, drop_existing=True):
    '''
        Creates table
        Params:
            columns_info = [ ['col_name', 'col_type'], .. ] # List of lists
    '''
    if drop_existing:
        query = f'DROP TABLE IF EXISTS "{table_name}"'
        try:
            db.execute(query)
        except sqlite3.Error as ex:
            print(ex)
            return False

    query = f'CREATE TABLE "{table_name}" ('
    query += ', '.join([f'"{x[0]}" {x[1]}' for x in columns_info])
    query += ')'
    try:
        cursor = db.execute(query)
        return True
    except sqlite3.Error as ex:
        print(ex)
    return False

def write_data(db, data, query):
    try:
        db.executemany(query, data)
        return True
    except (sqlite3.Error,OverflowError) as ex:
        print(ex)
    return False

def set_column_names(columns_info, reader, data):
    '''Read 1 row and guess the INTEGER and FLOAT values, else all is TEXT'''
    for row in reader:
        for i, col_data in enumerate(row):
            try:
                int(col_data)
                columns_info[i][1] = 'INTEGER'
            except ValueError:
                try:
                    float(col_data)
                    columns_info[i][1] = 'REAL'
                except ValueError:
                    pass
        data.append(row)
        break

def copy_bytes(source_f, dest_f, source_offset, data_size):
    '''Copies bytes from source_f to dest_f in chunks'''
    if data_size == 0:
        return
    chunk_size = 50000000 # 50 Mb
    source_f.seek(source_offset)

    while data_size > 0:
        data = source_f.read(min(chunk_size, data_size))
        dest_f.write(data)
        data_size -= len(data)

def sanitize_remove_nulls(path, temp_folder):
    '''Open csv file, remove nulls if present and save to new file.
       Returns a new file path to use. This fixes an issue with 
       MftECmd where sometimes nulls appear in the output csv file.
    '''
    file_size = os.path.getsize(path)
    if file_size == 0:
        return path
    with open(path, 'rb') as f:
        if os.name == 'nt':
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        else:
            mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        cleaned_file_path = os.path.join(temp_folder, os.path.basename(path) + '.nulls_removed')
        out_f = open(cleaned_file_path, 'wb')
        size_written = 0
        start = 0
        for match in re.finditer(b'[\x00]+', mm):
            end = match.start()

            size_to_copy = end - start
            copy_bytes(f, out_f, start, size_to_copy)
            size_written += size_to_copy

            start = match.end()

        if file_size == start: # File ended with nulls, no more data to copy
            pass
        elif start == 0: # No nulls found
            pass
        else: # nulls were present, copy from end of last null to end of file
            copy_bytes(f, out_f, start, file_size - start)
        out_f.flush()
        out_f.close()
        if size_written == 0: # nothing was written, delete empty file
            try:
                os.remove(cleaned_file_path)
            except OSError as ex:
                print(f'Failed to remove empty file : {cleaned_file_path} Error was:', str(ex))
            return path
        return cleaned_file_path

def import_csv(csv_file_path, db_path, table_name='', delimiter=',', append_existing_table=False, guess_column_types=False):
    try:
        db = sqlite3.connect(db_path)
        try:
            with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
                csv.field_size_limit(5 * 1024 * 1024) #5MB, else err beyond field > 131072 bytes
                reader = csv.reader(f, delimiter=delimiter)
                i = 0
                for row in reader: # read header row
                    headers = row
                    #print(row)
                    break
                if not table_name:
                    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]

                columns_info = [ [h, 'TEXT'] for h in headers]
                data = []
                if guess_column_types:
                    set_column_names(columns_info, reader, data)
                    i += len(data)
                try:
                    if table_exists(db, table_name):
                        if append_existing_table:
                            print(f'Existing table {table_name} found, will append to it')
                        else:
                            # drop table and create new
                            print(f'Existing table {table_name} found, will DROP it and recreate')
                            create_table(db, table_name, columns_info, True)
                    else:
                        create_table(db, table_name, columns_info, False)
                except sqlite3.Error as ex:
                    print(f'Error trying to check for table "{table_name}"', str(ex))
                    db.close()
                    return False
                
                query = f'INSERT INTO "{table_name}" VALUES (?' + ',?'*(len(columns_info) - 1) + ')'
                for row in reader: # continue reading to get data rows
                    data.append(row)
                    i += 1
                    if i % 200000 == 0:
                        if not write_data(db, data, query):
                            break
                        data.clear()
                if data:
                    write_data(db, data, query)
                db.commit()
                db.close()
                return True
        except csv.Error as ex:
            print("CSV error")
            print(str(ex))
    except (sqlite3.Error, OSError) as ex:
        print(f"Failed to create db at {db_path}")
        print(str(ex))
    return False

def main():
    usage = \
    """
(c) 2021 Yogesh Khatri,  @SwiftForensics
This script will read a csv file and convert it to an sqlite database. 
It can guess data types (TEXT, INT, REAL, ..) by looking at the first 
row of data if guess_column_types is True. By default, all columns are
set to TEXT.
    """

    parser = argparse.ArgumentParser(description='CSV to SQLITE tool', epilog=usage, 
                formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('csv_file_path', help='Path to csv file')
    parser.add_argument('-t', '--table_name', default='', help='Table name')
    parser.add_argument('-g', '--guess_column_types', action='store_true', help='guess column from first row (default False)')
    parser.add_argument('-o', '--output_path', help='Output file name and path')
    parser.add_argument('-s', '--skip_checks', action='store_true', help='Skips checking & creation of null free csv')
    parser.add_argument('-d', '--delete_existing', action='store_true', help='Delete existing db and create new one (default is write to existing)')
    parser.add_argument('-r', '--tsv_file', action='store_true', help='Input is TSV file instead of csv')
    parser.add_argument('-a', '--append_existing_table', action='store_true', help='Do not delete existing table with same name (default is to drop existing same name table)')
    args = parser.parse_args()

    csv_file_path = args.csv_file_path
    table_name = args.table_name

    if args.output_path:
        db_file_path = args.output_path
        temp_folder = os.path.split(os.path.realpath(args.output_path))[0]
    else:
        db_file_path = csv_file_path + ".sqlite"
        temp_folder = os.path.split(os.path.realpath(csv_file_path))[0]

    if not os.path.exists(csv_file_path):
        print(f"Error, input file does not exist: {csv_file_path}")

    if args.delete_existing and os.path.exists(db_file_path):
        print(f'Deleting existing db at {db_file_path}')
        os.remove(db_file_path) # delete existing

    delimiter = ','
    if args.tsv_file:
        delimiter = '\t'

    if args.skip_checks:
        cleaned_csv_path = csv_file_path
    else:
        # Check for nulls
        print('Checking and removing any nulls from file...')
        cleaned_csv_path = sanitize_remove_nulls(csv_file_path, temp_folder)

    print(f'Importing "{cleaned_csv_path}" to Sqlite now...')
    if not table_name:
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    result = import_csv(cleaned_csv_path, db_file_path, table_name, delimiter, args.append_existing_table, args.guess_column_types)
    if (not args.skip_checks) and cleaned_csv_path != csv_file_path:
        try:
            os.remove(cleaned_csv_path)
        except OSError as ex:
            print(f'Failed to remove temp file : {cleaned_csv_path} Error was:', str(ex))
    
    print("Converted successfully!" if result else "Failed! See errors above.")

if __name__ == "__main__":
    main()