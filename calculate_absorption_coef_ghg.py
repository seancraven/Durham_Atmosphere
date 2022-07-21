"""
    _summary_ Script to calculate absorbtion coefficient by altitude for 6
    most abundent GHGs.
    
    
    The main work is handeld by the hitran api.
    Calculating the absorption coefficients once
    and building a database after that enables much faster
    atmospheric model calculations after initial setup.
"""
import os
import sys
from typing import Tuple
import sqlite3
from sqlite3 import Error
import numpy as np
from tqdm import tqdm
import isa
from optical_depth_utilities import optical_depth


class HiddenPrints:
    """Surpresses prints to console"""

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


with HiddenPrints():
    import hapi


class Ghg:
    """
    records ghg abundance in ppb ghg abundance assumed constant. Up to implemented
    cealing of 10km.

    This is the same assumption as taken by MODTRAN which achieves accuracies
    better than 1k accuraciy on thermal brightness temperature.
    """

    ppm = {"CO2": 411 * 1000, "CH4": 1893, "N2O": 327, "H2O": 25 * 10**6}
    ids = {"CO2": 2, "CH4": 6, "N2O": 4, "H2O": 1}


def ghg_lbl_download():
    """
    Downloads and Stores Line by line Data for 4 most abundent ghg.
    if further gases are required add name and HITRAN id to
    molecule_id_dict. Data Is collected from HITRAN.

    assumes only most abundent isotopologue is required.
    """
    with HiddenPrints():
        hapi.db_begin("./spectral_line.db")
        isotopologue = 1  # only want main isotopologue
        min_wavenumber = 0
        max_wavenumber = 4000  ## spectral flux density(watts m^(-2) m^-1) is negliable beyond this region
        for gas, _id in Ghg.ids.items():
            hapi.fetch(gas, _id, isotopologue, min_wavenumber, max_wavenumber)


def ghg_od_calculate(gas: str, alt: float):
    """
    Calculates the optical density of a km of atmosphere, due to a single gas.

    Args:
        gas (str): string of gas name, valid gasses are found in Ghg.
        alt (float): midpoint altitude of km block of atmosphere.

    Returns:
        (np.array, np.array): wavenumber and optical density arrays of same shape.
    """
    temp = isa.get_temperature(alt)
    pressure = isa.get_pressure(alt)
    press_0 = isa.get_pressure(0)
    with HiddenPrints():
        nu, coef = hapi.absorptionCoefficient_Voigt(
            SourceTables=gas,
            Environment={"T": temp, "p": pressure / press_0},
            Diluent={"air": 1.0},
        )

        od = optical_depth(alt - 500, alt + 500, Ghg.ppm[gas], coef)
    return nu, od


def create_connection(db_file: str):
    """
    Creates a connection to database object.

    Args:
        db_file (str): /path/to/database/name.db

    Returns:
        database connection object:
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)


def create_table(conn: sqlite3.Connection, create_table_sql: str):
    """
    Creates a table from sql statement.

    Args:
        conn: Database to create table in.
        create_table_sql: SQL command to create table string.
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_gas(conn: sqlite3.Connection, task: Tuple):
    """
    Adds a gas to gases tabel.

    Args:
        conn: Database to insert into
        task: Tuple of values to insert into columns: mol_id, mol_name, mol_ppm.

    Returns:
        int: Last row number appended to table
    """

    sql = """INSERT INTO gases(mol_id,mol_name,mol_ppm)
            VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()

    return cur.lastrowid


def pragma(conn: sqlite3.Connection):
    """Fucntion to improve performance of sqlite database"""
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = 0;")
    conn.execute("PRAGMA cache_size = 1000000;")  # give it a GB
    conn.execute("PRAGMA locking_mode = EXCLUSIVE;")
    conn.execute("PRAGMA temp_store = MEMORY;")


def gas_query(conn: sqlite3.Connection, mol_id: int, alt: float, nu: float):
    """
    Function that performs a query on the optical depth table for a specific row
    corresponding to the arguments passed

    Args:
        conn: Database to query.
        mol_id: molecule id from Ghg class
        alt: midpoint altitude
        nu: wavenumber

    Returns:
        tuple: values from colums in tuple:(mol_id, altitude, wave_no, abs_coef)
    """

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
    ghg_lbl_download()
    #
    conn = create_connection(r"./optical_depth.db")
    # Use sqlite3 PRAGMA for faster loading
    pragma(conn)
    if conn is not None:
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
        create_table(conn, create_table_gas_sql)
        create_table(conn, create_table_abs_coef_sql)
        print("created tabels")
    else:
        print("Didnt Work")
    #
    # Populating Gases
    for (mol_name, mol_id), (_, mol_ppm) in zip(Ghg.ids.items(), Ghg.ppm.items()):
        gas_entry = (mol_id, mol_name, mol_ppm)
        try:
            create_gas(conn, gas_entry)
        except sqlite3.IntegrityError:
            cur = conn.cursor()
            sql = "SELECT mol_name from gases WHERE mol_name = ?;"
            cur.execute(sql, (mol_name,))
            added_mols = cur.fetchall()
            print(f"{added_mols} already in database")
    altitudes = np.arange(500, 30500, 1000)
    #
    # Populating optical_depths
    for gas, _id in tqdm(Ghg.ids.items()):
        for alt in tqdm(altitudes, leave=False):
            cur = conn.cursor()
            query = cur.execute(
                "SELECT * from optical_depths WHERE mol_id = ? and altitude = ?;",
                (_id, alt),
            )
            if not query.fetchall():
                wave_number, od = ghg_od_calculate(gas, alt)
                for nu, tau in zip(wave_number, od):
                    try:
                        conn.execute(
                            "INSERT INTO optical_depths VALUES(?,?,?,?)",
                            (_id, alt, nu, tau),
                        )
                        conn.commit()
                    except sqlite3.IntegrityError:
                        querey = gas_query(conn, _id, alt, nu)
                        if not querey:
                            print(
                                f"""Failed to add \n
                                molecule:{gas} \n
                                altitude:{alt} \n
                                wavenumber:{nu}
                                """
                            )
            else:
                pass


if __name__ == "__main__":
    main()
