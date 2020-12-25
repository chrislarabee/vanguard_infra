import os
import logging as log
from typing import Callable
from pathlib import Path
import json
from datetime import datetime as dt

import pandas as pd
import datagenius as dg

import constants, util as u

class PrepData:
    """
    Whenever you need to prep raw data before doing anything with 
    Tensorflow, create a prep function and use PrepData to execute it on
    the data in your input_data directory for the model. This class is 
    intended to make it convenient to process large datafiles in batches 
    and to store that file into a sql database. Ideal for situations 
    where you need to derive additional columns from your input data but 
    also want to be able to easily test a function that derives that 
    data.

    Args:
        prep_func (Callable): A function, which must accept two
            arguments: a pandas DataFrame and a datagenius 
            GeniusMetadata object. 
        model_dir (str): The name of the target directory in models.
        batch_size (int): The # to divide the raw input data into,
            default is 100,000. Pick a # that your PC can efficiently 
            process in memory.
    """
    def __init__(
            self, 
            prep_func: Callable, 
            batch_size: int = 100000):
        self._func = prep_func
        self._prep_cache = constants.SIM.joinpath('prep_cache.json')
        self.batch_size: int = batch_size
        self._header: list = None
        self._chunks = 1
        self._rows_processed = 0

    def execute(self, file_name: str, col_map: dict = None):
        """
        Executes the prep function on the target file (which must be 
        found in your model directory's datastore/input_data directory)
        in batches. After each successful batch the results will be 
        cached and saved, so that if you stop mid-execution and start 
        again later, you won't have to restart from the beginning of 
        your data file.

        Args:
            file_name (str): The name of the data file to prep.
            col_map (str): A dictionary produced by 
                lib.import_column_map, if your file contains columns you 
                want to drop.
        """
        if self._prep_cache.exists():
            self.load_cache()
        ignore = col_map['ignored'] if col_map else None
        print(f'Processing file {file_name} in chunks of {self.batch_size}')
        if '.csv' in file_name:
            file_name, _ = os.path.splitext(file_name)
        p = constants.RAW.joinpath(f'{file_name}.csv')
        chunks = 1
        start = dt.now()
        u.print_bar()
        for raw in pd.read_csv(p, chunksize=self.batch_size):
            if chunks < self._chunks: 
                print(
                    f'Skipping chunk {chunks} (Rows {self._rows_processed}) to '
                    f'{self._rows_processed + len(raw)} it has been processed '
                    f'in a previous session.', end='\r'
                )
            else:
                print(
                    f'Processing chunk {chunks} (Rows {self._rows_processed} to '
                    f'{self._rows_processed + len(raw)}). Total runtime = '
                    f'{dt.now() - start}.'
                )
                u.print_bar()
                chunk_start = dt.now()
                if self._header is None:
                    h, _ = dg.standardize_header(raw.columns)
                    self._header = h
                    raw.columns = h
                print('Beginning preprocessing...')
                step_start = dt.now()
                raw, md = raw.genius.preprocess()
                print(f'Preprocess complete. Runtime = {dt.now() - step_start}')
                print(f'Applying {self._func.__name__}...')
                raw, md = self._func(raw, md)
                if ignore:
                    print('Removing ignored columns...')
                    raw.drop(columns=ignore, inplace=True)
                print(f'Writing chunk to {constants.SIM}/datasets db...')
                raw.genius.to_sqlite(constants.SIM, file_name, drop_first=False)
                print(
                    f'Chunk {chunks} processed (Rows {self._rows_processed} to '
                    f'{self._rows_processed + len(raw)}). '
                    f'Runtime={dt.now() - chunk_start}')
                print(f'Saving cache...')
                self._chunks += 1
                self.save_cache()
                u.print_bar()
            self._rows_processed += len(raw)
            chunks += 1
        print(f'Data preparation complete. Total runtime = {dt.now() - start}')

        
    def load_cache(self):
        """
        Loads a saved PrepData cache from the sim_db directory.
        """
        with open(self._prep_cache, 'r') as r:
            c = json.load(r)
        for k, v in c.items():
            setattr(self, '_' + k, v)

    def save_cache(self):
        """
        Saves the PrepData cache to the sim_db directory.
        """
        c = dict(
            header=self._header, 
            chunks=self._chunks
        )
        with open(self._prep_cache, 'w') as w:
            json.dump(c, w)

