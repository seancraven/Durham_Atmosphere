'''
    _summary_ Script to calculate absorbtion coefficient by altitude for 6 
    most abundent GHGs.
    
    
    The main work is handeld by the hitran api. 
    Calculating the absorption coefficients once
    and building a database after that enables much faster 
    atmospheric model calculations after initial setup. 
'''
import sqlite3
from sqlite3 import Error
import isa
import numpy as np
import os, sys
from optical_depth_utilities import optical_depth
from tqdm import tqdm


class HiddenPrints:

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


with HiddenPrints():
    import hapi


class Ghg:
    '''
    records ghg abundance in ppb ghg abundance assumed constant. Up to implemented
    cealing of 10km. 
    
    This is the same assumption as taken by MODTRAN which achieves accuracies
    better than 1k accuraciy on thermal brightness temperature.
    '''
    ppm = {'CO2': 411 * 1000, 'CH4': 1893, 'N2O': 327, 'H2O': 25 * 10**6}
    ids = {'CO2': 2, 'CH4': 6, 'N2O': 4, 'H2O': 1}


def ghg_lbl_download():
    ''' Downloads and Stores Line by line Data for 4 most abundent ghg.
        if further gases are required add name and HITRAN id to 
        molecule_id_dict. Data Is collected from HITRAN.
        
        assumes only most abundent isotopologue is required.
    '''
    hapi.db_begin("./spectral_line.db")
    isotopologue = 1  # only want main isotopologue
    min_wavenumber = 0
    max_wavenumber = 4000  ## spectral flux density(watts m^(-2) m^-1) is negliable beyond this region
    for gas, id in Ghg.ids.items():
        with HiddenPrints():
            hapi.fetch(gas, id, isotopologue, min_wavenumber, max_wavenumber)


def ghg_od_calculate(gas, alt):

    temp = isa.get_temperature(alt)
    pressure = isa.get_pressure(alt)
    press_0 = isa.get_pressure(0)
    with HiddenPrints():
        nu, coef = hapi.absorptionCoefficient_Voigt(SourceTables=gas,
                                                    Environment={
                                                        "T": temp,
                                                        "p": pressure / press_0
                                                    },
                                                    Diluent={"air": 1.0})
    with HiddenPrints():
        od = optical_depth(alt - 500, alt + 500, Ghg.ppm[gas], coef)
    return nu, od


def create_connection(db_file):
    """ create a database connection to a SQLite database. 
        db_file is a path of the form:  
        /path/to/file/name_of_database.db
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_task(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = """INSERT INTO gases(mol_id,mol_name,mol_ppm)
            VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()

    return cur.lastrowid


def pragma(connection):
    connection.execute('PRAGMA journal_mode = OFF;')
    connection.execute('PRAGMA synchronous = 0;')
    connection.execute('PRAGMA cache_size = 1000000;')  # give it a GB
    connection.execute('PRAGMA locking_mode = EXCLUSIVE;')
    connection.execute('PRAGMA temp_store = MEMORY;')


def gas_query(mol_id, alt, nu):

    sql = """SELECT * from optical_depths
        WHERE mol_id = ? and
        WHERE altitude = ? and
        WHERE wave_no = ?;
        """
    cur = conn.cursor()
    cur.execute(sql, (mol_id, alt, nu))
    query_result = cur.fetchall()
    return query_result


def main():
    with HiddenPrints():
        ghg_lbl_download()
    create_table_gas_sql = """CREATE TABLE IF NOT EXISTS gases (
                                mol_id integer PRIMARY KEY, 
                                mol_name text NOT NULL, 
                                mol_ppm real NOT NULL
                                );"""
    create_table_abs_coef_sql = """CREATE TABLE IF NOT EXISTS optical_depths (
                                    mol_id INTEGER, 
                                    altitude REAL NOT NULL, 
                                    wave_no  REAL NOT NULL,
                                    abs_coef REAL NOT NULL,
                                    PRIMARY KEY (mol_id, altitude, wave_no),
                                    FOREIGN KEY (mol_id) REFERENCES gases (mol_id)
                                );"""

    conn = create_connection(r'./test.db')
    # Use sqlite3 PRAGMA for faster loading
    pragma(conn)
    if conn is not None:
        create_table(conn, create_table_gas_sql)
        create_table(conn, create_table_abs_coef_sql)
        print('created tabels')
    else:
        print('Didnt Work')
    # Populating Gases
    for (mol_name, mol_id), (_, mol_ppm) in zip(Ghg.ids.items(),
                                                Ghg.ppm.items()):
        mol_task = (mol_id, mol_name, mol_ppm)
        try:
            create_task(conn, mol_task)
        except sqlite3.IntegrityError:
            cur = conn.cursor()
            sql = "SELECT mol_name from gases WHERE mol_name = ?;"
            cur.execute(sql, (mol_name,))
            added_mols = cur.fetchall()
            print(f"{added_mols} already in database")
    altitudes = np.arange(500, 30500, 1000)
    for gas, ids in tqdm(Ghg.ids.items()):
        for alt in tqdm(altitudes, leave=False):
            cur = conn.cursor()
            query = cur.execute(
                "SELECT * from optical_depths WHERE mol_id = ? and altitude = ?;",
                (ids, alt))
            if not query.fetchall():
                wave_number, od = ghg_od_calculate(gas, alt)
                for nu, tau in zip(wave_number, od):
                    try:
                        conn.execute(
                            "INSERT INTO optical_depths VALUES(?,?,?,?)",
                            (ids, alt, nu, tau))
                        conn.commit()
                    except sqlite3.IntegrityError:
                        querey = gas_query(ids, alt, nu)
                        if not querey:
                            print(f'''Failed to add \n
                                molecule:{gas} \n
                                altitude:{alt} \n
                                wavenumber:{nu}
                                ''')
            else:
                pass


if __name__ == "__main__":
    main()
