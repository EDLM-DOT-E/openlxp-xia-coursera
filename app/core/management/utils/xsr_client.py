import hashlib
import logging

import numpy as np
import pandas as pd
from openlxp_xia.management.utils.xia_internal import get_key_dict

from core.models import XSRConfiguration

logger = logging.getLogger('dict_config_logger')


def read_source_file():
    """setting file path from s3 bucket"""
    xsr_data = XSRConfiguration.objects.first()
    file_name = xsr_data.source_file

    # TODO: Eventually we want to be able to do all sheets at once in extraction
    extracted_data = pd.read_excel(file_name,
                                    sheet_name="8 Test",
                                    engine='openpyxl')

    std_source_df = extracted_data.where(pd.notnull(extracted_data),
                                         None)
    source_nan_df = std_source_df.replace(np.nan, None)

    # Strip leading or trialing whitespcae for every column 
    source_nan_df.columns = source_nan_df.columns.map(str.strip)

    # Strip leading or trailing whitespace for every string element
    for i in source_nan_df.columns:
        if source_nan_df[i].dtype == 'object':
            check = True
            for x in source_nan_df[i]:
                if not isinstance(x, str):
                    check = False
            if check:
                source_nan_df[i] = source_nan_df[i].map(str.strip)

    #  Creating list of dataframes of sources
    source_list = [source_nan_df]

    logger.debug("Sending source data in dataframe format for EVTVL")
    # file_name.delete()
    return source_list


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """

    key_value = data_dict['Course Provider'].replace(' ','_') + '-' + str(data_dict['Course ID']).replace(' ','_') + '-DOTE-000'

    # Key value hash creation for source metadata
    key_value_hash = hashlib.sha512(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key
