from typing import Optional, List, Union, Tuple
import math
import re
import datetime as dt

from sqlalchemy.orm import Session
import numpy as np
import pandas as pd

from vanguard.db.models import CensusBlock, Voter, Call
from vanguard.db import constants


def donation_dt_to_datetime(d_date: str) -> dt.datetime:
    """
    Takes a donation date string and converts it to a datetime object.

    Args:
        d_date (str): A date string with dates separated by /
    Returns:
        datetime: A datetime object based on d_date, or d_date if no
            pattern match is found.
    """
    patterns = {
        r"\d{2}/\d{2}/\d{4}": "%m/%d/%Y",
        r"\d{2}/\d{4}": "%m/%Y",
        r"\d{4}": "%Y",
    }
    result = dt.datetime(0, 0, 0)
    for re_p, dt_fmt in patterns.items():
        if re.search(re.compile(re_p), d_date):
            result = dt.datetime.strptime(d_date, dt_fmt)
            break
    return result


def unpack_donation_col(donation_col: str) -> Tuple[float, float, int]:
    """
    Unpacks a string of donation data in the format of
    {$1@date1,$2@date2} into a dictionary of metadata on that string.

    Args:
        donation_col (str): A string of donation data.
    Returns:
        tuple: A tuple of metadata about the passed donation data.
    """
    output = (0, 0, -1)
    donation_col = re.sub(r"[{}$]", "", donation_col)
    if donation_col != "":
        donations = [x.split("@") for x in donation_col.split(",")]
        amts = [float(x[0]) for x in donations]
        last_dt = donation_dt_to_datetime(donations[0][1])
        output = (
            float(sum(amts)),
            float(np.mean(amts)),
            (dt.datetime.now() - last_dt).days,
        )
    return output


def prep_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpacks the dem donation data for each row in the raw district_data
    DataFrame and determines whether or not they are a donor. Performs a
    small amount of cleanup on the party_affiliation column.
    -
    Args:
        df (DataFrame): A pandas DataFrame with a demdonationamounts
            column.
    -
    Returns:
        (DataFrame): The transformed DataFrame.
    """
    df[["total", "avg", "days_since"]] = pd.DataFrame(
        df["demdonationamounts"].apply(unpack_donation_col).tolist(), index=df.index
    )
    df["party_affiliation"] = df["party_affiliation"].fillna("X")
    df["is_donor"] = df["total"].apply(lambda x: x > 0).astype("int")
    return df


def gen_call_data(
    session: Session,
    pos_resp_rate: Optional[float] = 0.1,
    num_samples: Optional[int] = None,
    batch_size: Optional[int] = 250000,
) -> None:
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
        print(
            f"Processing batch {i + 1} of {num_batches} (rows {start} " f"to {end})...",
            end="\r",
        )
        batch = (
            session.query(Voter, Voter.ohvfid)
            .filter(Voter.id >= start)
            .filter(Voter.id <= end)
            .all()
        )
        df = pd.DataFrame(batch)
        x = df.sample(frac=pos_resp_rate).index.tolist()
        df["result"] = df.index.isin(x).astype(int)
        calls = df.apply(
            lambda row: Call(ohvfid=row.ohvfid, call_result=row.result), axis=1
        )
        session.add_all(list(calls))
        session.commit()
        start += batch_size
        end += batch_size
    print("\nAll batches successfully processed.")


def prep_cenblock_training_data(
    session: Session,
    num_samples: Optional[int] = None,
    batch_size: Optional[int] = 250000,
) -> None:
    if num_samples is None:
        num_samples = session.query(CensusBlock).count()
    first_batch = True
    write_mode = "w"
    rows_processed = 0
    query = session.query(CensusBlock)
    for df in pd.read_sql(query.statement, session.bind, chunksize=batch_size):
        print(
            f"Processing rows {rows_processed + 1} to "
            f"{rows_processed + len(df)}...",
            end="\r",
        )
        df["donor_pct"] = df["total_donors"] / df["totalpop"]
        df.drop(columns=["id", "blockgeoid", "total_donors"], inplace=True)
        df.to_csv(
            constants.TRAIN.joinpath("cenblocks.csv"),
            header=first_batch,
            mode=write_mode,
            index=False,
        )
        write_mode = "a"
        first_batch = False
        rows_processed += len(df)
        if rows_processed >= num_samples:
            break
    print("\nAll rows successfully processed.")


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
    do_func = dict(
        total_donors="SUM(is_donor)",
        donation_total="SUM(total)",
    )
    do_nothing = ["blockgeoid"]
    select_cols = []
    cenblocks_cols = CensusBlock.gen_column_list()
    cenblocks_cols.pop(0)  # Remove id column.
    for k in cenblocks_cols:
        if k in do_nothing:
            select_cols.append(k)
        elif k in do_func.keys():
            select_cols.append(do_func[k])
        else:
            select_cols.append(f"MAX({k})")
    insert = gen_insert_table("cenblocks", cenblocks_cols)
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
    v_cols.pop(0)  # Remove id column.
    insert = gen_insert_table("voters", v_cols)
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
    table_name: str, columns: List[str], group_by: Optional[List[str]] = None
) -> str:
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
    group_by = f" GROUP BY {', '.join(group_by)}" if group_by else ""
    return f"{base} FROM {table_name}{group_by};"
