import argparse
import logging
import hashlib

import pandas as pd

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def main(filename):
    logger.info('Starting cleaning data')
    dataf = _read_data(filename)
    dataf = _modify_column_title(dataf)
    dataf = _generate_uid_for_rows(dataf)
    dataf = _drop_rows_with_missin_data(dataf)
    _save_data(dataf, filename)
    ##return dataf


def _read_data(filename):
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename)


def _modify_column_title(dataf):
    logger.info('modify column title')
    title_nan = dataf['title'].isna()
    missing_title = (dataf[title_nan]['website'].str.extract(r'(?P<missing_title>[^/]+)$')
                     .applymap(lambda title: title.split('.'))
                     .applymap(lambda title_new: list(map(lambda title_n: title_n.replace('www', ''), title_new)))
                     .applymap(lambda title_new: list(map(lambda title_n: title_n.replace('com', ''), title_new)))
                     .applymap(lambda title_new: ' '.join(title_new))
                     )

    dataf.loc[title_nan, 'title'] = missing_title.loc[:, 'missing_title']
    logger.info('The column_id is {}'.format(title_nan))
    logger.info('The missing_title is {}'.format(missing_title))
    return dataf


def _new_column(dataf, column_id):
    logger.info('Filling new column in dataframe')
    dataf['new_column_id'] = column_id

    return dataf


def _generate_uid_for_rows(dataf):
    logger.info('generating for each row')
    uids = (dataf
            .apply(lambda row: hashlib.md5(bytes(row['website'].encode())), axis=1)
            .apply(lambda row: row.hexdigest())
            )
    dataf['uid'] = uids
    dataf.set_index('uid')

    return dataf


def _remove_duplicate_entries(dataf, column_name):
    logger.info('Removing duplicte entries')
    dataf.drop_duplicate(subset=[column_name], keep='first', inplace=True)

    return dataf


def _drop_rows_with_missin_data(dataf):
    logger.info('Dropping rows with missin data')

    return dataf.dropna()


def _save_data(dataf, filename):
    clean_filename = 'clean{}'.format(filename)
    logger.info('Saving data at location: {}'.format(clean_filename))
    dataf.to_csv(clean_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='The path to de dirty data', type=str)
    arg = parser.parse_args()
    dataf = main(arg.filename)
    print(dataf)
