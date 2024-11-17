import numpy as np
from datetime import datetime,timedelta
import matplotlib.pyplot as plt

def yy2yyyy(yy):
    if yy < 30:
        yyyy = "20" + str(yy)
    else:
        yyyy = "19" + str(yy)
    return int(yyyy)

def y2kSec2datetime( y2kSec ) :
    """
    converts an int or float y2ksecond time into a datetime.datetime object
    """
    # Convert to datetime obj
    constY2K = datetime(2000, 1, 1, 12) # 2000-01-01 12:00:00
    return constY2K + timedelta(seconds=y2kSec)
        
def sinexToSec(myString):
    '''
    Converts a sinex string (e.g. 15:141:64800) to
    y2kseconds
    
    :param myString: sinex-formmated time 
    :type myString: str
    
    
    :returns: the time in y2k seconds, or None for 00:000:00000
    :rtype: float or None
    '''
    year,doy,seconds=myString.split(':')
    year=int(year);doy=int(doy); seconds=int(seconds)
    if year+doy+seconds==0:
        return None
    else:
        if year<100:
            year=yy2yyyy(year)
        myDateTime=datetime(year,1,1)+\
                   timedelta(doy-1+seconds/86400.0)
        return y2ksecs(myDateTime)
    
def sec2date(y2ksecs,fmt='%Y-%m-%d %H:%M:%S.%f'):
    '''
    Converts seconds past 2000-01-01 12:00.0000 to a string 
    YYYY-MM-DD HH:MM:SS.ssssss

    To get just integer (compatible with StaDB) fmt = '%Y-%m-%d %H:%M:%S'

    :Example:
    >>> from dateUtils import sec2date
    >>> print(sec2date(375969900))
    2011-12-01 00:05:00.000000
    >>> print(sec2date(375969900, fmt='%Y-%m-%d %H:%M:%S'))
    2011-12-01 00:05:00
    '''
    return y2kSec2datetime(y2ksecs).strftime(fmt)

def y2ksecs( d ):
    '''
    return seconds past 2000-01-01 12:00:00 from a datetime. Note
    if this is a GPS calendar time this is our standard for elapsed
    seconds past 2000-01-01 11:59:47 UTC

    :Example:
    >>> print y2ksecs( datetime(2014, 6,3,11,20,0,0) )
    455066400.0
    '''
    constY2K = datetime(2000, 1, 1, 12) # 2000-01-01 12:00:00
    return (d - constY2K).total_seconds()

def date2yyyyDoyStr( dateObj, prefix = False ) :
    ''' 
    Return 4 digit year and 3 digit day of year (DOY)
    as strings for the input date object.
     
    
    :param dateObj: the date to convert
    :type dateObj: datetime.date
    :param prefix: if True, 'y' and 'd' are pre-pended to the year and DOY strings, respectively. [Default = False]
    :type prefix: bool
     
    :returns: year string, day string
    :rtype: <str>,<str>
    '''
    try :
        # Get year and day of year as strings
        y, d = dateObj.strftime("%Y %j").split()
        if prefix:
            return ( 'y' + y, 'd' + d )
         
        return ( y, d )

    # Catch errors
    except:
        raise ValueError( "Error in dateUtils")

class Trop2DataReader:
    
    def __init__(self, file):
        self.file = file
        self.trop = {}
        self.solution_block = False
        
    def new_time(self, e):
        '''
        Converts the time into decimal years.
        '''
        yy, ddd, sssss = e.split(":")
        yy = int(yy)
        ddd = int(ddd)
        sssss = int(sssss)
        
        if yy > 50:
            base_year = 1900
        else:
            base_year = 2000

        days_in_year = 365

        if (base_year + yy) % 4 == 0:
            days_in_year += 1

        decimal_year = base_year + yy + ((ddd - 1 + (sssss / 86400)) / days_in_year)
        return decimal_year

    def parse_file(self):
        '''
        Parses file 
        '''
        for line in self.file:
            line = line.decode('utf-8')
            line = line.strip()
            if line.startswith('*'):
                continue
            if not self.solution_block:
                if 'TROPO PARAMETER NAMES' in line:
                    troParams = line.split()[3:]
                    for p in range(len(troParams)):
                        if 'STDEV' in troParams[p]:
                            troParams[p] = troParams[p-1] + 'STDEV'
                elif 'TROPO PARAMETER UNITS' in line:
                    troUnits = line.split()[3:]
                elif '+TROP/SOLUTION' in line:
                    self.solution_block = True
            elif '-TROP/SOLUTION' in line:
                break
            else:
                cols = line.split()
                e = cols[1]
                epoch = self.new_time(e)
                if cols[0] not in self.trop:
                    self.trop[cols[0]] = {}
                self.trop[cols[0]][epoch] = {}
                for field in range(len(troParams)):
                    candidate = float(cols[field + 2])
                    if candidate > -99.9:
                        self.trop[cols[0]][epoch][troParams[field]]=float(cols[field+2])#*float(troUnits[field])
                        
        return self.trop


                        
    def make_lst(self):
        '''
        Creates a list in order to feed into a dataframe.
        '''
        data = self.parse_file()
        # station =  list(data.keys())
        data = data[list(data.keys())[0]]
        dates = list(data.keys())
        zwd_values = [entry.get('TROWET', np.nan) for entry in data.values()]
        # trotot_values = [entry.get('TROTOT', np.nan) for entry in data.values()]
        # trototstdev_values = [entry.get('TROTOTSTDEV', np.nan) for entry in data.values()]
        # tgnwet_values = [entry.get('TGNWET', np.nan) for entry in data.values()]
        # tgnwetstdev_values = [entry.get('TGNWETSTDEV', np.nan) for entry in data.values()]
        # tgewet_values = [entry.get('TGEWET', np.nan) for entry in data.values()]
        # tgewetstdev_values = [entry.get('TGEWETSTDEV', np.nan) for entry in data.values()]
        # iwv_values = [entry.get('IWV', np.nan) for entry in data.values()]
        # press_values = [entry.get('PRESS', np.nan) for entry in data.values()]
        # temdry_values = [entry.get('TEMDRY', np.nan) for entry in data.values()]
        datetime_values = [datetime(int(value), 1, 1) + timedelta(days=(value - int(value)) * 365) for value in dates]
        
        # station = station *len(tgewetstdev_values)
        return zwd_values, datetime_values 
    