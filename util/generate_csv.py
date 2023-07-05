""" CSV FILE GENERATOR
To generate test csv files within empty cells, call python functions

generate_csv(lines)
modify_empty_cells()
"""
import uuid
import random
import csv
import pandas as pd

FILENAME = '../data/mock_big_register.csv'


def _add_headers():
    headers = "id,firstname,lastname,email,email2,phone,profession\n"
    empty_file = open(FILENAME, 'w')
    empty_file.write(headers)
    empty_file.close()


def _get_occupations():
    occupations = []
    with open('occupations.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            occupations.append(row['Occupations'])
    return occupations


def _generate_line(professions):
    name = f'name{random.randrange(1000)}'
    surname = f'surname{random.randrange(1000)}'
    email = f'{name}@email.com'
    email2 = f'{name}@email2.com'
    phone = random.randrange(100000, 999999)
    profession = professions[random.randrange(len(professions))]
    return f'{uuid.uuid4()}, {name}, {surname}, {email}, {email2}, {phone}, {profession}\n'


def generate_csv(lines):
    """
    Generate a test csv file specifying how many lines
    :param lines: quantity of lines the csv should have
    """
    _add_headers()
    occupation_list = _get_occupations()

    with open(FILENAME, 'a') as f:
        for i in range(lines):
            line = _generate_line(occupation_list)
            f.write(line)

    print('Successfully generated')


def modify_empty_cells():
    df = pd.read_csv(FILENAME)

    num_row, num_col = df.shape

    for i in range(100):
        x = random.randrange(num_row)
        y = random.randrange(1, num_col)
        df.loc[x, df.columns[y]] = None

    empty = df.isna().sum()
    print('Checking empty cells', empty)  # .to_csv()
    df.to_csv(FILENAME)

    print(f'Test csv file generated: {FILENAME}')


generate_csv(1000000)
modify_empty_cells()
