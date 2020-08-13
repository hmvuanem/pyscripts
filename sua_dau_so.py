import re
import numpy as np

dau_so = {
    '086':'086',
    '096':'096',
    '097':'097',
    '098':'098',
    '0162':'032',
    '0163':'033',
    '0164':'034',
    '0165':'035',
    '0166':'036',
    '0167':'037',
    '0168':'038',
    '0169':'039',
    '032':'032',
    '033':'033',
    '034':'034',
    '035':'035',
    '036':'036',
    '037':'037',
    '038':'038',
    '039':'039',
    '089':'089',
    '090':'090',
    '093':'093',
    '0120':'070',
    '0121':'079',
    '0122':'077',
    '0126':'076',
    '0128':'078',
    '070':'070',
    '079':'079',
    '077':'077',
    '076':'076',
    '078':'078',
    '088':'088',
    '091':'091',
    '094':'094',
    '0123':'083',
    '0124':'084',
    '0125':'085',
    '0127':'081',
    '0129':'082',
    '083':'083',
    '084':'084',
    '085':'085',
    '081':'081',
    '082':'082',
    '092':'092',
    '056':'056',
    '058':'058',
    '099':'099',
    '0199':'059',
    '059':'059'
}

def remove_non_numeric(x):
    """
    Remove all non-numeric characters
    """
    x = str(x)
    x = re.sub('[^0-9]', '', x)
    return x

def valid_phone(x):
    x = str(x)
    if x == 'nan':
        x = np.nan
    else:
        changed = 0
        for key, value in dau_so.items():
            if x.startswith(key):
                x = x
                changed = 1
                break
        if changed == 0:
            x = np.nan
    return x

def fix_head(x):
    '''
    Change phone number from 11 number to 10 number
    '''
    x = str(x)
    if x == 'nan':
        x = np.nan
    else:
        x = str(x)
        changed = 0
        for key, value in dau_so.items():
            if x.startswith(key):
                x = value + x[len(key):]
                changed = 1
                break
        if changed == 0:
            x = np.nan
    return x

def change_phone(x):
    '''
    0, Remove all non-numeric characters
    1, Remove all phone number has less than 9 number
    2, Remove all phone has more than 12 number
    3, Change or add number '0' to phone
    4, Convert phone from 11 to 10 number
    '''
    x = remove_non_numeric(x)
    if len(x) < 9:
        x = np.nan
    elif len(x) == 9:
        x = '0'+ x
    elif len(x) == 10:
        if x.startswith('0'):
            x = x
        else: x = '0' + x
    elif len(x) == 11:
        if x.startswith('0'):
            x = x
        elif x.startswith('84'):
            x = '0' + x[2:]
        else: x = '0' + x
    elif len(x) == 12:  
        if x.startswith('0'):
            x = np.nan
        elif x.startswith('84'):
            x = '0' + x[2:]
        else: x = np.nan
    else: x = np.nan
    return x