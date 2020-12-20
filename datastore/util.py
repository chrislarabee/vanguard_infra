import os
from pathlib import Path
from typing import Dict

import pandas as pd

        
def import_column_map(template_dir: str, **custom_types) -> Dict[str, list]: 
    """
    Convenience function for importing a model's col_map template. 
    Default metatype markers are:
        N for numeric.
        C for categorical.
        Y for label.
        X for ignore.

    Args:
        template_dir (str): Path to the model's template dir, which must
            contain a csv file named col_map.
        custom_types: If desired, supply custom metatype markers from 
            your col_map file and their corresponding metatype names.
            Columns you've marked with those markers will be sorted into
            those types. 
    Returns:
        Dict[str, list]: A dictionary with the column values from 
            col_map sorted into lists based on metatype.
    """
    if '.csv' in template_dir:
        p = Path(template_dir)
    else:
        p = Path(template_dir, 'col_map.csv')
    cm = pd.read_csv(str(p))
    if 'column' not in cm.columns:
        raise ValueError('col_map must contain a column named "column"')
    if 'metatype' not in cm.columns:
        raise ValueError('col_map must contain a column named "metatype"')
    cm = cm.to_dict('records')
    result = {
        'numeric': [], 'categorical': [], 'ignored': [], 'label': [], 'all': [], 
        **{v: [] for v in custom_types.values()}
    }
    _map = {
        'N': 'numeric', 'C': 'categorical', 'X': 'ignored', 'Y': 'label',
        **custom_types
    }
    for r in cm:
        result[_map[r['metatype']]].append(r['column'])
        if r['metatype'] != 'X':
            result['all'].append(r['column'])
    return result


def print_bar():
    print('=' * os.get_terminal_size()[0])