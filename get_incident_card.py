import datetime
import logging
import re
from typing import List, Dict

from jinja2 import Environment, FileSystemLoader
import pandas as pd


logging.basicConfig(filename='get_incident_card.log', level=logging.DEBUG)


def read_data(incident_data_path: str, revenue_data_path: str) -> (pd.DataFrame, pd.DataFrame):
    """
    Reads data from incident csv file and revenue csv file
    :param incident_data_path: path to file
    :param revenue_data_path: path to file
    :return: tuple of read dataframes
    """
    return pd.read_csv(incident_data_path, sep=';'), pd.read_csv(revenue_data_path, sep=';')


def merge_dataframes(left_df, right_df, left_keys: List, right_keys: List) -> pd.DataFrame:
    """
    Merging dataframes
    :param left_df:
    :param right_df:
    :param left_keys: keys for merging from left df
    :param right_keys: keys for merging from right df
    :return: merged dataframes
    """
    return pd.merge(left_df, right_df,
                    left_on=left_keys,
                    right_on=right_keys)


def extract_numbers(inp_str: str) -> List:
    """
    Extracts numbers from given string
    :param inp_str:
    :return: numbers from string
    """
    return re.findall(r'\d+', inp_str)[0]


def change_date_format(date_str, format_str):
    return date_str.strftime(format_str)


def transform_data(df: pd.DataFrame) -> Dict:
    """
    Check if immutable columns have only one value and extract its. Creates dict with needed values
    :param df: dataframe with one id of incident
    :return: dict with values for incident car
    """
    immutable_cols = ['Начало аварии', 'Конец аварии', 'Точка отказа Сервис', 'Точка отказа Система',
                      'Точка отказа Проект', 'Затронуло', 'SLA', 'Обратились', 'Краткое описание аварии',
                      'Детальное описание аварии', 'Плановые работы', 'Недоступность']
    df.fillna('N/A', inplace=True)
    values_for_card = {}

    if all(df.groupby('Порядковый номер аварии')[immutable_cols].nunique().eq(1).all(axis='columns')):
        values_for_card = {
            'idx': df['Порядковый номер аварии'][0],
            'dt_begin': datetime.datetime.strptime(df['Начало аварии'][0], '%Y-%m-%dT%H:%M:%S.%f'),
            'dt_end': datetime.datetime.strptime(df['Конец аварии'][0], '%Y-%m-%dT%H:%M:%S.%f'),
            'service_machine': df['Точка отказа Сервис'][0],
            'system': df['Точка отказа Система'][0],
            'project': df['Точка отказа Проект'][0],
            'service': '<br>'.join(list(df['Услуга'].unique())),
            'description': re.sub(r'\{(.*?)\}', '', df['Краткое описание аварии'][0]).replace('  ', ''),
            'planned_works': df['Плановые работы'][0],
            'accessibility': df['Недоступность'][0],
            'details': df['Детальное описание аварии'][0],
            'number_inj': extract_numbers(df['Затронуло'][0]),
            'number_called': extract_numbers(df['Обратились'][0]),
            'sla': extract_numbers(df['SLA'][0]),
            'compensation': sum(df['% компенсации'] * df['Выручка'] / 100),
            'customers': df.sort_values(by=['Выручка'], ascending=False)[['Customer_care_user_id', 'Статус']][:15],
        }

        values_for_card['dt_duration'] = str(values_for_card['dt_end'] - values_for_card['dt_begin'])
        values_for_card['dt_begin'] = change_date_format(values_for_card['dt_begin'], '%d/%m/%Y <br> %H:%M:%S')
        values_for_card['dt_end'] = change_date_format(values_for_card['dt_end'], '%d/%m/%Y <br> %H:%M:%S')
        values_for_card['compensation'] = f'{values_for_card["compensation"]:.2f}'
    else:
        logging.error('Immutable values are not unique')

    return values_for_card


def load_data(card_values: Dict, template_path: str = 'template.html') -> None:
    """
    Save rendered the html file with extracte values to current directory
    :param card_values: dict with values
    :param template_path: path to html template
    :return: None
    """
    if card_values:
        env = Environment(loader=FileSystemLoader('./templates'))
        template = env.get_template(template_path)
        output_from_parsed_template = template.render(**card_values)
        with open(f'card_{card_values["idx"]}.html', 'w') as f:
            f.write(output_from_parsed_template)


def process(incident_path: str, revenue_path: str) -> None:
    """
    Extract, transform and load data
    :param incident_path:
    :param revenue_path:
    :return:
    """
    incident_df, revenue_df = read_data(incident_path, revenue_path)
    logging.info('Files read')
    df = merge_dataframes(incident_df, revenue_df,
                          ['Customer_care_user_id'],
                          ['Customer_care_user_id'])
    incident_idxs = df['Порядковый номер аварии'].unique()

    for idx in incident_idxs:
        card_values = transform_data(df[df['Порядковый номер аварии'] == idx])
        logging.info('Data transformed')
        load_data(card_values)
        logging.info('Data saved')


if __name__ == '__main__':
    try:
        process('data/incident_data.csv',
                'data/revenue.csv')
    except Exception as ex:
        logging.error('Fatal error', ex)
