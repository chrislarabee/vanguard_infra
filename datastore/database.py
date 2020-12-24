from typing import Dict, List, Optional
from sqlite3 import Cursor
import math
from random import random

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import as_declarative
import pandas as pd

from . import util as u

@as_declarative()
class Base:
    @classmethod
    def gen_column_list(cls) -> List[str]:
        """
        Returns:
            List[str, ...]: A list of strings, the names of all the 
                columns of the model.
        """
        return [i.key for i in sa.inspect(cls).mapper.column_attrs]


class CensusBlock(Base):
    __tablename__ = 'cenblocks'

    blockgeoid = Column(Integer, primary_key=True)
    totalpop = Column(Integer)
    total_donors = Column(Float)
    donation_total = Column(Float)
    percentunder18 = Column(Float)
    percent18to19 = Column(Float)
    percent20 = Column(Float)
    percent21 = Column(Float)
    percent22to24 = Column(Float)
    percent25to29 = Column(Float)
    percent30to34 = Column(Float)
    percent35to39 = Column(Float)
    percent40to44 = Column(Float)
    percent45to49 = Column(Float)
    percent50to54 = Column(Float)
    percent55to59 = Column(Float)
    percent60to61 = Column(Float)
    percent62to64 = Column(Float)
    percent65to66 = Column(Float)
    percent67to69 = Column(Float)
    percent70to74 = Column(Float)
    percent75to79 = Column(Float)
    percent80to84 = Column(Float)
    percent85andup = Column(Float)
    percent25to44 = Column(Float)
    percent40to59 = Column(Float)
    percentcollegeeducated = Column(Float)
    percentunder10k = Column(Float)
    lowpercentunder10k = Column(Float)
    highpercentunder10k = Column(Float)
    percent10kto14k = Column(Float)
    lowpercent10to14k = Column(Float)
    highpercent10to14k = Column(Float)
    percent15kto19k = Column(Float)
    lowpercent15to19k = Column(Float)
    highpercent15to19k = Column(Float)
    percent20kto24k = Column(Float)
    lowpercent20to24k = Column(Float)
    highpercent20to24k = Column(Float)
    percent25kto29k = Column(Float)
    lowpercent25to29k = Column(Float)
    highpercent25to29k = Column(Float)
    percent30kto34k = Column(Float)
    lowpercent30to34k = Column(Float)
    highpercent30to34k = Column(Float)
    percent35kto39k = Column(Float)
    lowpercent35to39k = Column(Float)
    highpercent35to39k = Column(Float)
    percent40kto44k = Column(Float)
    lowpercent40to44k = Column(Float)
    highpercent40to44k = Column(Float)
    percent45kto49k = Column(Float)
    lowpercent45to49k = Column(Float)
    highpercent45to49k = Column(Float)
    percent50kto59k = Column(Float)
    lowpercent50to59k = Column(Float)
    highpercent50to59k = Column(Float)
    percent60kto74k = Column(Float)
    lowpercent60to74k = Column(Float)
    highpercent60to74k = Column(Float)
    percent75kto99k = Column(Float)
    lowpercent75to99k = Column(Float)
    highpercent75to99k = Column(Float)
    percent100kto124k = Column(Float)
    lowpercent100to124k = Column(Float)
    highpercent100to124k = Column(Float)
    percent125kto149k = Column(Float)
    lowpercent125to149k = Column(Float)
    highpercent125to149k = Column(Float)
    percent150kto199k = Column(Float)
    lowpercent150to199k = Column(Float)
    highpercent150to199k = Column(Float)
    percent200kandup = Column(Float)
    lowpercent200kandup = Column(Float)
    highpercent200kandup = Column(Float)
    percentabove100k = Column(Float)
    lowpercentabove100k = Column(Float)
    highpercentabove100k = Column(Float)
    percent50kto99k = Column(Float)
    lowpercent50kto99k = Column(Float)
    highpercent50kto99k = Column(Float)
    percentemployerbasedonly = Column(Float)
    percentdirectpurchaseonly = Column(Float)
    percentmedicareonly = Column(Float)
    percentemployeranddirectpurchase = Column(Float)
    percentemployerbasedandmedicare = Column(Float)
    percentmedicareandmedicaid = Column(Float)
    percentnohealthinsurance = Column(Float)

    def __repr__(self):
        return (
            f"<Cenblock(blockgeoid={self.blockgeoid}), totalpop={self.totalpop}>"
        )


class Voter(Base):
    __tablename__ = 'voters'
    
    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    suffix = Column(String)
    party_affiliation = Column(String)
    street1 = Column(String)
    street2 = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(Integer) 
    plus4 = Column(Float)
    # Some voters have more than 1 precinct/district_num.
    # 'precinct = Column(String)
    # 'district_num = Column(Integer) 
    ohvfid = Column(String)
    blockgeoid = Column(Integer) 
    demdonationamounts = Column(String)
    demcommitteecodes = Column(String)
    repdonationamounts = Column(String)
    repcommitteecodes = Column(String)
    otherpartydonationamounts = Column(String)
    otherpartycommitteecodes = Column(String)
    total = Column(Float)
    avg = Column(Float)
    days_since = Column(Integer)
    is_donor = Column(Integer)

    def __repr__(self):
        return (
            f"<Voter(id={self.id}, name={self.first_name} {self.last_name})>"
        )


class Call(Base):
    __tablename__ = 'calls'

    id = Column(Integer, primary_key=True)
    ohvfid = Column(String)
    call_result = Column(Integer)


class District(Base):
    __tablename__ = 'districts'

    district_num = Column(Integer, primary_key=True)
    district = Column(String)
    totalpop = Column(Integer)
    total_donors = Column(Float)
    donation_total = Column(Float)


def gen_call_data(
        session: Session,
        pos_resp_rate: Optional[float] = 0.1,
        num_samples: Optional[int] = None,
        batch_size: Optional[int] = 250000) -> None:
    """
    Generates simulated call data and loads into the database connected 
    via the passed Session.
    -
    Args:
        session (Session): A SQLAlchemy Session object.
        pos_resp_rate (Optional[float], optional): The overall positive 
            response rate you want to simulate. This percentage of your 
            number of samples will be positive. Defaults to 0.1.
        num_samples (Optional[int], optional): The number of samples you
            want to generate. Defaults to None, which will mean that the 
            entire voter table will be used.
        batch_size (Optional[int], optional): The size of the batches 
            you want to pull from the voter table. Use this if your 
            voter table is very large. Defaults to 250000, or 
            num_samples if that is lower.
    """
    if num_samples is None:
        num_samples = session.query(Voter).count()
    start = 1
    end = batch_size if batch_size < num_samples else num_samples
    num_batches = math.ceil(num_samples / batch_size)
    for i in range(num_batches):
        print(f'Processing batch {i} of {num_batches}...')
        batch = session.query(Voter, Voter.ohvfid).\
                filter(Voter.id>=start).\
                filter(Voter.id<=end).all()
        df = pd.DataFrame(batch)
        x = df.sample(frac=pos_resp_rate)
        df['result'] = df.apply(
            lambda row: 1 if row.index.values[0] in x.index else 0,
            axis=1
        )
        calls = df.apply(
            lambda row: Call(ohvfid=row.ohvfid, call_result=row.result), 
            axis=1)
        session.add_all(list(calls))
        start += batch_size
        end += batch_size
    session.commit()


def gen_populate_cenblocks(source_table: str) -> str:
    """
    Convenience function for generating the insert statement needed to 
    populate the cenblocks table from the prepped raw data table.
    -
    Args:
        source_table (str): The name of the table to pull data from.
    Returns:
        str: A SQL insert and select statement as a str, tailored to the 
            needs of the cenblocks table.
    """
    do_sum = dict(total_donors='SUM(is_donor)', donation_total='SUM(total)') 
    do_nothing = ['blockgeoid']
    select_cols = []
    cenblocks_cols = CensusBlock.gen_column_list()
    for k in cenblocks_cols:
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
    -
    Args:
        source_table (str): The name of the table to pull data from.
    Returns:
        str: A SQL insert and select statement as a str, tailored to the 
            needs of the voters table.
    """
    v_cols = Voter.gen_column_list()
    v_cols.pop(0) # Remove id column.
    insert = gen_insert_table('voters', v_cols)
    select = gen_select(source_table, v_cols, v_cols)
    return f"{insert} {select}"


def gen_insert_table(table_name: str, columns: List[str]) -> str:
    """
    Convenience method for generating an insert statement based on one 
    of the dictionaries above.
    -
    Args:
        table_name (str): The name of the table to insert into. 
        columns (List[str]): A list of column names to insert values 
            into. 
    -
    Returns:
        str: A SQL insert statement as a str.
    """
    return f"INSERT INTO {table_name} ({', '.join(columns)}) "


def gen_select(
        table_name: str, 
        columns: List[str],
        group_by: Optional[List[str]] = None) -> str:
    """
    Convenience function for generating a select statement based on a 
    passed dictionary.
    -
    Args:
        table_name (str): The name of the table to select from. 
        columns (List[str]): A list of SQL strings valid as column 
            values to select.
        group_by (Optional[List[str]], optional): A list of SQL strings 
            valid as columns to group by. Defaults to None.
    -
    Returns:
        str: A SQL select statement as a str.
    """
    base = f"SELECT {', '.join(columns)}"
    group_by = f" GROUP BY {', '.join(group_by)}" if group_by else ''
    return f'{base} FROM {table_name}{group_by};'
