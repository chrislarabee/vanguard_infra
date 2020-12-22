from typing import Dict, List, Optional
from sqlite3 import Cursor

cenblocks_cols = {
    'blockgeoid': 'INTEGER',
    'totalpop': 'INTEGER',
    'total_donors': 'FLOAT',
    'donation_total': 'FLOAT',
    'percentunder18': 'FLOAT',
    'percent18to19': 'FLOAT',
    'percent20': 'FLOAT',
    'percent21': 'FLOAT',
    'percent22to24': 'FLOAT',
    'percent25to29': 'FLOAT',
    'percent30to34': 'FLOAT',
    'percent35to39': 'FLOAT',
    'percent40to44': 'FLOAT',
    'percent45to49': 'FLOAT',
    'percent50to54': 'FLOAT',
    'percent55to59': 'FLOAT',
    'percent60to61': 'FLOAT',
    'percent62to64': 'FLOAT',
    'percent65to66': 'FLOAT',
    'percent67to69': 'FLOAT',
    'percent70to74': 'FLOAT',
    'percent75to79': 'FLOAT',
    'percent80to84': 'FLOAT',
    'percent85andup': 'FLOAT',
    'percent25to44': 'FLOAT',
    'percent40to59': 'FLOAT',
    'percentcollegeeducated': 'FLOAT',
    'percentunder$10k': 'FLOAT',
    'lowpercentunder$10k': 'FLOAT',
    'highpercentunder$10k': 'FLOAT',
    'percent$10kto$14k': 'FLOAT',
    'lowpercent$10to$14k': 'FLOAT',
    'highpercent$10to$14k': 'FLOAT',
    'percent$15kto$19k': 'FLOAT',
    'lowpercent$15to$19k': 'FLOAT',
    'highpercent$15to$19k': 'FLOAT',
    'percent$20kto$24k': 'FLOAT',
    'lowpercent$20to$24k': 'FLOAT',
    'highpercent$20to$24k': 'FLOAT',
    'percent$25kto$29k': 'FLOAT',
    'lowpercent$25to$29k': 'FLOAT',
    'highpercent$25to$29k': 'FLOAT',
    'percent$30kto$34k': 'FLOAT',
    'lowpercent$30to$34k': 'FLOAT',
    'highpercent$30to$34k': 'FLOAT',
    'percent$35kto$39k': 'FLOAT',
    'lowpercent$35to$39k': 'FLOAT',
    'highpercent$35to$39k': 'FLOAT',
    'percent$40kto$44k': 'FLOAT',
    'lowpercent$40to$44k': 'FLOAT',
    'highpercent$40to$44k': 'FLOAT',
    'percent$45kto$49k': 'FLOAT',
    'lowpercent$45to$49k': 'FLOAT',
    'highpercent$45to$49k': 'FLOAT',
    'percent$50kto$59k': 'FLOAT',
    'lowpercent$50to$59k': 'FLOAT',
    'highpercent$50to$59k': 'FLOAT',
    'percent$60kto$74k': 'FLOAT',
    'lowpercent$60to$74k': 'FLOAT',
    'highpercent$60to$74k': 'FLOAT',
    'percent$75kto$99k': 'FLOAT',
    'lowpercent$75to$99k': 'FLOAT',
    'highpercent$75to$99k': 'FLOAT',
    'percent$100kto$124k': 'FLOAT',
    'lowpercent$100to$124k': 'FLOAT',
    'highpercent$100to$124k': 'FLOAT',
    'percent$125kto$149k': 'FLOAT',
    'lowpercent$125to$149k': 'FLOAT',
    'highpercent$125to$149k': 'FLOAT',
    'percent$150kto$199k': 'FLOAT',
    'lowpercent$150to$199k': 'FLOAT',
    'highpercent$150to$199k': 'FLOAT',
    'percent$200kandup': 'FLOAT',
    'lowpercent$200kandup': 'FLOAT',
    'highpercent$200kandup': 'FLOAT',
    'percentabove$100k': 'FLOAT',
    'lowpercentabove$100k': 'FLOAT',
    'highpercentabove$100k': 'FLOAT',
    'percent$50kto$99k': 'FLOAT',
    'lowpercent$50kto$99k': 'FLOAT',
    'highpercent$50kto$99k': 'FLOAT',
    'percentemployerbasedonly': 'FLOAT',
    'percentdirectpurchaseonly': 'FLOAT',
    'percentmedicareonly': 'FLOAT',
    'percentemployeranddirectpurchase': 'FLOAT',
    'percentemployerbasedandmedicare': 'FLOAT',
    'percentmedicareandmedicaid': 'FLOAT',
    'percentnohealthinsurance': 'FLOAT'
}

district_cols = {
    'district_num': 'INTEGER',
    'district': 'VARCHAR',
    'totalpop': 'INTEGER',
    'total_donors': 'FLOAT',
    'donation_total': 'FLOAT'
}

voter_cols = {
    'first_name': 'VARCHAR',
    'middle_name': 'VARCHAR',
    'last_name': 'VARCHAR',
    'suffix': 'VARCHAR',
    'party_affiliation': 'VARCHAR',
    'street1': 'VARCHAR',
    'street2': 'VARCHAR',
    'city': 'VARCHAR',
    'state': 'VARCHAR',
    'zip': 'INTEGER',
    'plus4': 'FLOAT',
    'precinct': 'VARCHAR',
    'district_num': 'INTEGER',
    'ohvfid': 'VARCHAR',
    'blockgeoid': 'INTEGER', 
    'demdonationamounts': 'VARCHAR',
    'demcommitteecodes': 'VARCHAR',
    'repdonationamounts': 'VARCHAR',
    'repcommitteecodes': 'VARCHAR',
    'otherpartydonationamounts': 'VARCHAR',
    'otherpartycommitteecodes': 'VARCHAR',
    'total': 'FLOAT',
    'avg': 'FLOAT',
    'days_since': 'INTEGER',
    'is_donor': 'INTEGER'
}


def gen_populate_cenblocks(source_table: str) -> str:
    """
    Convenience function for generating the insert statement needed to 
    populate the cenblocks table from the prepped raw data table.

    Args:
        source_table (str): The name of the table to pull data from.
    Returns:
        str: A SQL insert and select statement as a str, tailored to the 
            needs of the cenblocks table.
    """
    do_sum = dict(total_donors='SUM(is_donor)', donation_total='SUM(total)') 
    do_nothing = ['blockgeoid']
    select_cols = []
    for k in cenblocks_cols.keys():
        if k in do_nothing:
            select_cols.append(k)
        elif k in do_sum.keys():
            select_cols.append(do_sum[k])
        else:
            select_cols.append(f'MAX({k})')
    insert = gen_insert_table('cenblocks', cenblocks_cols)
    select = gen_select(source_table, select_cols, do_nothing)
    return f"{insert} {select}"


def gen_populate_voters(source_table: str) -> str:
    """
    Convenience function for generating the insert statement needed to 
    populate the voters table from the prepped raw data table.

    Args:
        source_table (str): The name of the table to pull data from.
    Returns:
        str: A SQL insert and select statement as a str, tailored to the 
            needs of the voters table.
    """
    insert = gen_insert_table('voters', voter_cols)
    select = gen_select(source_table, voter_cols.keys())
    return f"{insert} {select}"


def gen_create_table(table_name: str, column_dict: Dict[str, str]) -> str:
    """
    Convenience method for generating a create table statement based on 
    one of the dictionaries above.

    Args:
        table_name (str): The name of the table to create. 
        column_dict (Dict[str, str]): A dictionary with keys as column
            names and values as SQLite data type strings (e.g. INTEGER).

    Returns:
        str: A SQL create table statement as a str.
    """
    base = f"CREATE TABLE {table_name} ("
    for k, v in column_dict.items():
        base += f"{k + ' ' + v + ', '}"
    return base[:-2] + ');'


def gen_insert_table(table_name: str, column_dict: Dict[str, str]) -> str:
    """
    Convenience method for generating an insert statement based on one 
    of the dictionaries above.

    Args:
        table_name (str): The name of the table to insert into. 
        column_dict (Dict[str, str]): A dictionary with keys as column
            names.

    Returns:
        str: A SQL insert statement as a str.
    """
    return f"INSERT INTO {table_name} ({', '.join(column_dict.keys())}) "


def gen_select(
        table_name: str, 
        columns: List[str],
        group_by: Optional[List[str]] = None) -> str:
    """
    Convenience function for generating a select statement based on a 
    passed dictionary.

    Args:
        table_name (str): The name of the table to select from. 
        columns (List[str]): A list of SQL strings valid as column 
            values to select.
        group_by (Optional[List[str]], optional): A list of SQL strings 
            valid as columns to group by. Defaults to None.

    Returns:
        str: A SQL select statement as a str.
    """
    base = f"SELECT {', '.join(columns)}"
    group_by = f" GROUP BY {', '.join(group_by)}" if group_by else ''
    return f'{base} FROM {table_name}{group_by};'
