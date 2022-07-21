"""
    File to store basic functions for atmospheric calculations.
"""
import isa
from scipy.integrate import quad
from scipy import constants


def number_density(alt):
    """
    Returns Number of particles per m^3
    assuming ideal gas.

    Args:
        alt (float): Altitude in meters.

    Returns:
        float: n(alt) in molecules/m^3
    """
    mass_of_air = 28.9647 * 10**-3 / constants.N_A  # conver kg/m^3 to molecule/m^3
    return isa.get_density(alt) / mass_of_air


def particle_per_sq_m(alt_0, alt_1):
    """
    returns number of particles per square meter
    between two altitudes

    Args:
        alt_0 (float): altitude in meters
        alt_1 (float): altitude in meters

    Returns:
        float:
    """
    return quad(number_density, alt_0, alt_1)[0]


def optical_depth(alt_0, alt_1, ppm_conc, abs_coef):
    """
    Calculates optical depth m^-1, between two altitudes.
    This quantity is often referred to symbolically as tau.

    Args:
        alt_0 (float): altitude in meters
        alt_1 (float): altitude in meters
        ppm_conc (float): parts per million concentration of the gas
        abs_coef (np.array): absorbrion coefficient array

    Returns:
        np.array: optical depth of gas between two altitudes
    """
    particles = particle_per_sq_m(alt_0, alt_1) * ppm_conc * 10 ** (-9)
    return particles * abs_coef * 10 ** (-4)  # 10^-4 factor from cm^2->m^2 conv
