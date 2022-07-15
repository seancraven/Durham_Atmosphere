    '''
    _summary_ Script to calculate absorbtion coefficient by altitude for 6 
    most abundent GHGs.
    
    
    The main work is handeld by the hitran api. 
    Calculating the absorption coefficients once
    and building a database after that enables much faster 
    atmospheric model calculations after initial setup. 
    '''
    import hapi