import pyodbc as py
import pandas as pd
from threading import Thread
import logging
import os.path

# Fill these parameters

credentials = {
    "SERVER": "",
    "DATABASE": ""
}

log = logging.getLogger("Database")
logging.basicConfig(level=logging.INFO)


class ExportTableToCSVThread(Thread):
    """
    Export tables concurrently
    """

    def __init__(self, table, folder):
        Thread.__init__(self)
        self.table_name = table
        self.folder = folder

    def run(self):
        try:
            conn = get_connection()
            log.info("starting export for: " + self.table_name)
            file = get_file_name(self.folder, self.table_name)
            pd.read_sql('SELECT * FROM {}'.format(self.table_name), conn).to_csv(file, index=False)
            log.info("export complete for: " + self.table_name)
        except Exception as e:
            log.error("error exporting for:" + self.table_name)
            log.error(e)


def get_file_name(folder, file):
    return folder + "/" + file + ".csv"


def is_credentials_filled():
    return len(credentials['SERVER']) > 0 and len(credentials['DATABASE']) > 0


def get_connection():
    c = 'Driver={SQL Server};Server=' + credentials['SERVER'] + ';Database=' + credentials[
        'DATABASE'] + ';Trusted_Connection=yes;'
    try:
        conn = py.connect(c)
        return conn
    except Exception as e:
        print(e)
        return None


def get_table_list():
    conn = get_connection()
    if conn is None:
        return []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables = [row[2] for row in cursor]
    return tables


def export_data(tables, folder):
    if not tables:
        log.info("No tables found to export!")
        return False

    thread_list = []
    for table in tables:
        th = ExportTableToCSVThread(table, folder)
        thread_list.append(th)
        th.start()
    for th in thread_list:
        th.join()
    return True


def __get_size(filename):
    if os.path.isfile(filename):
        st = os.stat(filename)
        return st.st_size
    else:
        return 0


def check_files_exist(table_list, folder):
    log.info("validating files...")
    for table in table_list:
        file_name = get_file_name(folder, table)
        if not os.path.isfile(file_name):
            log.info("Expected file : " + file_name + " not found")
            return False
        else:
            if __get_size(file_name) == 0:
                log.info("Expected file : " + file_name + " has no data!")
                return False
    log.info("validation complete")
    return True


def create_folder():
    folder = 'db_' + credentials['DATABASE']
    try:
        os.mkdir(folder)
        return folder
    except FileExistsError:
        log.error("A folder called '{}' already exists".format(folder))
        return None


def process():
    if not is_credentials_filled():
        log.error("Please specify database name and server!")
    else:
        folder = create_folder()
        if folder is not None:
            tables = get_table_list()
            if not tables:
                log.info("No tables available to export")
            else:
                if export_data(tables, folder):
                    if check_files_exist(tables, folder):
                        log.info("process complete!")
                    else:
                        log.warning("not all data was exported!")


if __name__ == '__main__':
    process()
