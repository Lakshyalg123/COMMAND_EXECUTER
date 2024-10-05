import os
import csv
import re
from asyncio import wait_for
from datetime import datetime
from ScriptingSSH import ScriptingSSH
import mysql.connector
# import schedule
# import time

# Database Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'lakshya@123',
    'database': 'mavnirdatabase',
    'allow_local_infile': True
}


def connect_to_db():
    """Connect to the MySQL database."""
    return mysql.connector.connect(**db_config)


import re


def clean_log_data(log_lines, command_type):
    """Clean log data based on the command type."""
    cleaned_data = []

    for line in log_lines:
        columns = line.split()
        if command_type == 'df':
            # Check for the header or skip lines until we find the relevant data
            if len(columns) >= 6:  # Ensure there are enough columns
                # Assuming the following columns: Filesystem, Size, Used, Avail, Use%, Mounted on
                node = columns[0]  # Usually the filesystem or node name
                used = columns[2]   # Used space
                utilisation_percentage = columns[4].replace('%', '')  # Remove '%' from utilization percentage
                # Append the cleaned data in the order needed for the SQL table
                cleaned_data.append([node, utilisation_percentage, used])

        elif command_type == 'top':
            if len(columns) >= 10 and line[0].isdigit():
                try:
                    node = columns[1]

                    # Clean CPU data
                    cpu = re.sub(r'[^0-9]', '', columns[15])  # Remove all non-numeric characters
                    cpu = cpu.lstrip('0')  # Remove leading zeros
                    if cpu == '':  # Handle case where the CPU might have been all zeros
                        cpu = '0'

                    # Clean MEM data
                    mem = columns[7]
                    if 'K' in mem:
                        mem = mem.replace('K', '')  # Remove K
                    elif 'M' in mem:
                        mem = str(float(mem.replace('M', '')) * 1024)  # Convert M to KB

                    # Clean CPU% and MEM%
                    cpu_percent = columns[2].replace('%', '')
                    mem_percent = columns[14].replace('%', '')

                    cleaned_data.append([node, cpu, mem, cpu_percent, mem_percent])
                except IndexError:
                    continue  # Skip lines that don't match the expected format

    return cleaned_data



def write_to_csv(cleaned_data, csvfile_path, command_type):
    """Write cleaned data to a CSV file."""
    with open(csvfile_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        if command_type == 'df':
            csvwriter.writerow(['id','node', 'utilisation', 'used', 'insert_timestamp'])  # Adjusted
        elif command_type == 'top':
            csvwriter.writerow(['node', 'cpu', 'mem', 'cpu_percent', 'mem_percent'])

        for row in cleaned_data:
            csvwriter.writerow(row)

def load_csv_to_db(csvfile_path, command_type):
    """Load data from a CSV file into the database."""
    db_connection = connect_to_db()
    cursor = db_connection.cursor()

    if command_type == 'df':
        query = """
        LOAD DATA LOCAL INFILE '{}' 
        INTO TABLE tbl_df 
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 LINES 
        (id,node, utilisation, used, insert_timestamp);
        """.format(csvfile_path)

    elif command_type == 'top':
        query = """
        LOAD DATA LOCAL INFILE '{}' 
        INTO TABLE tbl_cpu_mem 
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 LINES 
        (node, cpu, mem, cpu_percent, mem_percent);
        """.format(csvfile_path)

    try:
        cursor.execute(query)
        db_connection.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        db_connection.close()


def execute_command(command, command_type):
    """Execute a command on the server and handle the output."""
    logfile_dir = os.getcwd()

    # Generate a unique logfile name
    # log_filename = generate_unique_filename('lakshya_log', 'log')
    log_filename=command_type+".log"
    logfile_path = os.path.join(logfile_dir, log_filename)

    # Generate a unique CSV file name
    # csv_filename = generate_unique_filename('lakshya_data', 'csv')
    csv_filename= command_type+".csv"
    csvfile_path = os.path.join(logfile_dir, csv_filename)

    try:
        # Set up SSH session and execute command
        obj = ScriptingSSH('192.168.1.2', 'lakshya', 'radha@123', slogfile=logfile_path)
        obj.connect()

        # Execute the command and wait for output
        obj.sendAndWait(command, "%|$|#", "|")

        # Ensure output is written to log file
        obj.disconnect()

        # Read the log file
        with open(logfile_path, 'r') as f:
            log_lines = f.readlines()

        # Clean and write data to CSV
        cleaned_data = clean_log_data(log_lines, command_type)
        write_to_csv(cleaned_data, csvfile_path, command_type)

        # Load data from CSV into the database
        load_csv_to_db(csvfile_path, command_type)

        # Print confirmation
        print(f"Data from '{command}' inserted into database successfully.")

    except Exception as e:
        print(f"Error executing command: {e}")


# def generate_unique_filename(base_name, extension):
#     """Generate a unique filename with the current timestamp."""
#     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#     return f"{base_name}_{timestamp}.{extension}"

# def schedule_tasks():
#     """Schedule the execution of commands at regular intervals."""
#     # Schedule the df -h command every 10 minutes
#     schedule.every(10).minutes.do(execute_command, "df -h", "df")
#
#     # Schedule the top command every 10 minutes
#     schedule.every(10).minutes.do(execute_command, "top -l 1", "top")
#
#     while True:
#         schedule.run_pending()  # Run the scheduled tasks
#         time.sleep(1)


# Example Commands
if __name__ == "__main__":
    # Execute and save data from 'df -h'
    execute_command("df -h", "df")

    # Execute and save data from 'top -n 1'
    execute_command("top -l 1","top")
    # print("Starting scheduler...")
    # schedule_tasks()
