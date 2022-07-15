'''
    _summary_ Script to calculate absorbtion coefficient by altitude for 6 
    most abundent GHGs.
    
    
    The main work is handeld by the hitran api. 
    Calculating the absorption coefficients once
    and building a database after that enables much faster 
    atmospheric model calculations after initial setup. 
'''
import hapi
import isa
import numpy as np
import os, sys
class HiddenPrints:
    '''Stops module functions printing ovemuch'''
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout
class GhgAbundance:
    '''
    records ghg abundance in ppb ghg abundance assumed constant. Up to implemented
    cealing of 10km.  
    
    This is the same assumption as taken by MODTRAN which achieves accuracies
    better than 1k accuraciy on thermal brightness temperature.
    '''
    CO2 = 411*1000
    CH4 = 1893
    N2O = 327
    H20 = 25 * 10**6

def ghg_lbl_download():
    ''' Downloads and Stores Line by line Data for 5 most abundent ghg.
        if further gases are required add name and HITRAN id to 
        molecule_id_dict. Data Is collected from HITRAN.
        
        assumes only most abundent isotopologue is required.
    '''
    hapi.db_begin("./spectral_line_db.db")
    isotopologue = 1 # only want main isotopologue
    min_wavenumber = 0
    max_wavenumber = 5000 ## spectral flux density(watts m^(-2) m^-1) is negliable beyond this region
    molecule_id_dict = {'H2O': 1, 'CO2': 2, 'CH4': 6, 'N2O': 4}
    for gas, id in molecule_id_dict.items():
        with HiddenPrints():
            hapi.fetch(gas,id, isotopologue, min_wavenumber, max_wavenumber)
        
def ghg_crossection_calculate():
    '''
    Calculate Crossections for main GHG
    '''
    trop_bound_alt = 10*1000
    atltitudes = np.arange(0,trop_bound_alt, 100)
    temp, pressure,  = isa.get_temperature(atltitudes), isa.get_pressure(atltitudes) 
    density = isa.get_density(atltitudes)
    ghg_gases = list(hapi.tableList())
    if 'sampletab' in ghg_gases: # Deals with bug in hapi.tableList()
        ghg_gases.remove('sampletab')
    for T, P, Dens in zip(temp,pressure, density):
        
        
        

if __name__ == "__main__":
    ghg_lbl_download()    
        
    print(GhgAbundance.CO2)
