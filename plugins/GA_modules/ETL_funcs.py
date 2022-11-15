import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
import numpy as np
import pandas as pd

from plugins.GA_modules.constants import POSTGRES_CONN_ID


def extract_func(
    university_id,
    sql_path,
    csv_path,
    logger,
    db_conn_id=POSTGRES_CONN_ID,
):
    """Read query statement from .sql file in INCLUDE_DIR, get data from
    postgres db and save it as .csv in FILES_DIR

    Parameters
    ----------
    university_id: string
        Unique per-university identifier to set names of files, custom-
        ize logging information, etc.  Default is local constant UNIVERS
        ITY_ID.
    sql_path: string or path object
        System path to the .sql file containing the sql query statement.
        Default is local constant SQL_PATH.
    csv_path: string or path object
        System path with location and name of the .csv file to be creat-
        ed.  Default is local constant CSV_PATH.
    db_conn_id: string
        Db connection id as configured in Airflow UI (Admin -> Connect-
        ions).  Default is plugins.constants.POSTGRES_CONN_ID.

    Returns
    -------
    None.
    """

    # Read sql query statement from .sql in INCLUDE_DIR
    with open(sql_path, 'r') as f:
        command = f.read()

    # Get hook, load data into pandas df and save it as .csv in FILES_DIR
    pg_hook = PostgresHook.get_hook(db_conn_id)
    df = pg_hook.get_pandas_df(command)
    df.to_csv(csv_path, index=False)

    logger.info(
        f'Extraction done: read {university_id}.sql in INCLUDE_DIR and '
        f'created {university_id}_select.csv in FILES_DIR')


def transform_func(csv_path, txt_path, **kwargs):
    """Perform several transformations over the original data.  The de-
    cription of the arguments is enough to see its functionality, but
    the age computation requires some criteria specification.  The func-
    tion assumes all inscription dates will be within the 21st century
    and all birthdates will be within the 20th and 21st centuries.  Vio-
    lating this will result in absurd computations.

    For the ambiguous date cases (two digit birth years spanning the
    20th and 21st centuries), these criteria is followed: (1) No upper
    limit for the age is considered as it would be statistically compro-
    mising to eliminate valid outliers.  Any extreme cases should be a-
    nalyzed at a further stage.  However, a min_age parameter is used to
    avoid cases like, for ex., newborns, etc.  (2) Any birth year that
    can  be assigned to the 21st century without violating the min_age
    parameter is assigned to it.  (3) Any birth year that cannot be as-
    signed to the 21st century (either because its two digits form a
    number larger than  the number formed by the last two digits of the
    inscription year, or because it would result in subjects younger
    than min_age) is assigned to the 20th century.

    Parameters
    ----------
    csv_path: string or path object
        System path with location and name of the .csv file to be creat-
        ed.  Default is local constant CSV_PATH.
    txt_path: string or path object
        System path with location and name of the .txt file to be creat-
        ed.  Default is local constant TXT_PATH.

    **kwargs
    --------
    to_lower: list or array-like
        Names of columns to turn to all lowercase.  If wrong values are
        passed, corresponding messages will be printed (available in the
        automatic Airflow log of the function).
    replace_underscores: list or array-like
        Names of columns on which to perform replacement of underscores
        with blank spaces.  If there are any trailing underscors, these
        are deleted prior to replacement.  If wrong values are passed,
        corresponding messages will be printed (available in the auto-
        matic Airflow log of the function).
    gender: dict
        Dictionary of length 2 containing the current gender values (the
        ones to be replaced) in its keys, and the values with which to
        replace them in its values.  Be careful to check the old values
        are as in the gender column, as passing strange values will re-
        sult in the one item value broadcasting over the whole column.
    date_format: {'%Y-%m-%d', '%d-%b-%y'}
        Date format for all date columns.  Note that in the second case
        the year is ambiguous.  Proper handling of this case is done au-
        tomatically by the function.  Age is automatically computed if
        this argument is passed(otherwise not).  Age computation requi-
        res the min_age kwarg.  If date_format is passed but min_age is
        not, a message will be printed (available in the automatic Air-
        flow log of the function) and the age will not be computed.  An-
        other message will be printed if the date format is incorrect or
        the date format of the columns are not supported.
    min_age: int
        Minimum age allowed for university enrollment.  This parameter
        must be passed if date_format also is, as they are used together
        to compute the ages.  If this argument is passed but date_format
        is not, age will not be computed with no warning.
    postal_data_path: string or path object
        If one of the columns postal_code or location is empty, a path
        to a new table containing full postal code and location data can
        be provided to fill the empty column.  This requires the fill
        kwarg.  If postal_data_path is passed but fill is not, a message
        will be printed (available in the automatic Airflow log of the
        function) and the age will not be computed.  Another message
        will be printed if the date format is incorrect or the date for-
        mat  of the columns is not supported.
    fill: {'postal_code', 'location'}
        Column to be filled.  Be careful to pass the right column name,
        as if the name passed is the one of the column already filled,
        its values will be lost and the other column will not be filled.
        This parameter must be passed if postal_data_path also is, as
        they are  used together to compute the ages.  If this argument
        is passed but date_format is not, age will not be computed with
        no warning.

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError if provided csv_path is invalid.
    OSError if provided txt_path is invalid.
    """

    df = pd.read_csv(csv_path, header=0, dtype='str')

    # Try processing gender values...
    try:
        df['gender'] = np.where(
            df['gender'].values == list(kwargs['gender'].keys())[0],
            list(kwargs['gender'].values())[0],
            list(kwargs['gender'].values())[1])
    # ... except arg. gender not passed
    except KeyError:
        pass
    # ... except passed value is other than dictionary
    except AttributeError:
        print('Only a dictionary is accepted for arg. gender. '
              f'Passed a {type(kwargs["gender"])}')
    else:
        df['gender'] = df['gender'].astype('category')

    # Try processing to_lower columns...
    try:
        for col in kwargs['to_lower']:
            # List comp. preferred over pandas methods for efficiency
            df[col] = [x.lower() for x in df[col].values]
    except KeyError as err:
        # ... except to_lower not passed
        if err.args[0] == 'to_lower':
            pass
        # ... except typo in to_lower
        else:
            print(f'Column does not exist: "{col}" in to_lower')
    # ... except passed column dtype is not str
    except AttributeError:
        print(f'Cannot convert nonstring data to lowercase: "{col}" in '
              'to_lower.')

    # Try processing replace_underscores columns...
    try:
        for col in kwargs['replace_underscores']:
            # First eliminate trailing underscores, then replace remain-
            # ing ones with blank spaces
            df[col] = [x.rstrip('_').replace('_', ' ') for x in df[col].values]
    except KeyError as err:
        # ... except replace_underscores not passed
        if err.args[0] == 'replace_underscores':
            pass
        # ... except typo in replace_underscores
        else:
            print(f'Column does not exist: "{col}" in replace_underscores')
    # ... except passed column dtype is not str
    except AttributeError:
        print(f'Cannot replace underscores in nonstring data: "{col}" in '
              'replace_underscores.')

    # Try performing age calculation...
    try:
        # If birth year has two digits (ambiguous birthdate), infer cen-
        # tury and compute ages
        if 'y' in kwargs['date_format']:
            # Get year from column fecha_nacimiento (two-digit int)
            birth_year = np.array([
                int(x[7:]) for x in df['fecha_nacimiento'].values
            ])
            # Get birth day and month
            birth_day_month = pd.to_datetime(
                [x[:6] for x in df['fecha_nacimiento'].values],
                format=kwargs['date_format'][:5],
                errors='coerce'
            ).values

            # If all values for birth day and month are NaT, then passed
            # date_format is wrong
            if pd.isnull(birth_day_month).all():
                raise ValueError

            # Get year from column inscription_date as two-digit int
            inscription_year = np.array(
                [int(x[7:]) for x in df['inscription_date'].values]
            )
            # Get unique inscription years values
            inscription_unique = np.unique(inscription_year)
            # Get inscription day and month
            inscription_day_month = pd.to_datetime(
                [x[:6] for x in df['inscription_date'].values],
                format=kwargs['date_format'][:5],
                errors='coerce'
            ).values

            # First, select all birth years that can be assigned to the
            # 20th century respecting min_age, perform addition to get
            # the four-digit int year.

            # Ex.: min_age = 15, inscription_unique = [20, 21].

            # A) For inscription year 20 (2020), 2020-15 = 5, then every
            # year from 2000 to 2004 results in ages higher than 15.
            # Therefore, it is looked for cases in which
            # (birth_year < 20 - kwargs['min_age'])
            #     & (inscription_year == 20) =
            # (birth_year < 20 - 15) & (inscription_year == 20) =
            # (birth_year < 5) & (inscription_year == 20).
            # Then, subjects born in 2000, 2001, 2002, 2003 or 2004 and
            # enrolled in 2000 are selected.  The same applies to in-
            # scription year 2021, just with the year changed:
            # (birth_year < 21 - kwargs['min_age']),
            #     & (inscription_year == 21)

            # B) Subjects enrolled in 2020 and born in 2020-15=2005 who
            # have already turned years by inscription_day_month in 2020
            # are 15 years old. Then, it is checked for cases in which
            # ((birth_year == 20 - kwargs['min_age'])
            #     & (inscription_day_month == 20)
            #     & (birth_day_month < inscription_day_month)) =
            # ((birth_year == 5)
            #     & (inscription_year == 20)
            #     & (birth_day_month < inscription_day_month)).
            # The same for 2021, with the year changed.

            # To allow for any number of enrollment years (inscription_
            # unique), as the conditions are always the same except for
            # int inscription year, two list comprehensions are used to
            # generate the boolean selections, which are stored as ar-
            # rays: The first list comp. (bool_select_1) generates con-
            # ditions of type A) for every inscription year, and the
            # second one (bool_select_2) generates conditions of type
            # B). Ex.:
            # bool_select_1 = array([[True, False, False, ... , True]
            #                        [False, False, True, ... , True]])

            # As one condition OR another must be True, using the any()
            # method on each column of the arrays returns the OR vector
            # result. In the above example:
            # bool_select_1 = ([True,  False, False, ... , True]
            #                  [False, False, True, ... ,  True]).any()
            #               =  [True,  False, True, ... ,  True]

            # Therefore, computing bool_select_1.any() | bool_select_2.
            # any() results in the final selection of subjects born in
            # the 21st century. The years are obtained by adding 2000.

            # Try creating boolean selections according to min_age...
            try:
                bool_select_1 = np.array([
                    (birth_year < y - kwargs['min_age'])
                        & (inscription_year == y)
                    for y
                    in inscription_unique
                ]).any(axis=0)
                bool_select_2 = np.array([
                    (birth_year == y - kwargs['min_age'])
                        & (inscription_year == y)
                        & (birth_day_month < inscription_day_month)
                    for y
                    in inscription_unique
                ]).any(axis=0)
            # ... except min_age not passed
            except KeyError:
                print(
                    'Argument min_age needed to compute the age. Pass min_age'
                    '=0 if minimum age is not required. Age not computed.'
                )
            else:
                birth_year = np.where(
                    bool_select_1 | bool_select_2,
                    birth_year + 2000,
                    birth_year + 1900
                )
                # Compute ages.  Condition is True = 1 when the subject
                # has not turned years by the time of the inscription,
                # so 1 year is subtracted
                df['age'] = inscription_year + 2000 - birth_year \
                            - (birth_day_month > inscription_day_month)
        # If date is unambiguous
        elif 'Y' in kwargs['date_format']:
            # Get birth_year as four-digit int
            birth_year = np.array([
                int(x[:4]) for x in df['fecha_nacimiento'].values
            ])

            # Get birth day and month
            birth_day_month = pd.to_datetime(
                [x[5:] for x in df['fecha_nacimiento'].values],
                format=kwargs['date_format'][3:],
                errors='coerce'
            ).values
            # If all values for birth day and month are NaT, then passed
            # date_format is wrong
            if pd.isnull(birth_day_month).all():
                raise ValueError

            # Get inscription_year as four-digit int
            inscription_year = np.array([
                int(x[:4]) for x in df['inscription_date'].values
            ])

            inscription_day_month = pd.to_datetime(
                [x[5:] for x in df['inscription_date'].values],
                format=kwargs['date_format'][3:],
                errors='coerce'
            ).values
            # Compute age. Use float as dtype to avoid problems when
            # None is assigned in next try clause
            age = (inscription_year - birth_year
                  - (birth_day_month > inscription_day_month)).astype('float')
            # Try eliminating registries less than min_age...
            try:
                age[age < kwargs['min_age']] = None
            # ... except min_age not passed
            except KeyError:
                print(
                    'Argument min_age needed to compute the age. Pass min_age'
                    '=0 if minimum age is not required. Age not computed.'
                )
            else:
                # Save in df
                df['age'] = age
                # Convert to pandas nullable int data type
                df['age'] = df['age'].astype('Int64')
    # ... except date_format not passed
    except KeyError:
        pass
    # ... except typo in date_format or unsupported date format in df
    except ValueError:
        print(
            'Problem with date format; either date_format is wrong, or the '
            'date format of the date columns is unsupported. Age not computed.'
        )
    else:
        # Delete unnecessary birthdate column
        df.drop('fecha_nacimiento', axis='columns', inplace=True)

    # Try processing postal data...
    try:
        postal_data = pd.read_csv(
            kwargs['postal_data_path'],
            header=0,
            names=['postal_code', 'location'],
            dtype='str'
        )
        # Save location/postal_code column position
        col_loc = df.columns.get_loc(kwargs['fill'])
    except KeyError as err:
        err = err.args[0]
        # ... except postal_data_path not passed
        if err == 'postal_data_path':
            pass
        # ... except arg. fill not passed
        elif err == 'fill':
            print('Arg. fill needed for postal data processing. '
                  'Location/postal codes not added.')
        # ... except arg. fill passed but with typo
        else:
            print(f'Column does not exist: "{kwargs["fill"]}" in arg. fill. '
                  'Location/postal codes not added.')
    # ... except typo in postal_data_path to .csv
    except FileNotFoundError:
        print('Wrong postal_data_path. No postal data added.')
    else:
        postal_data['location'] = [
            x.lower() for x in postal_data['location'].values
        ]
        postal_data.drop_duplicates(
            subset='location', keep='first', inplace=True
        )

        # Delete empty location/postal_code column
        df.drop(kwargs["fill"], axis='columns', inplace=True)
        if kwargs['fill'] == 'postal_code':
            # Merge
            df = df.merge(postal_data, how='left', on='location')
        elif kwargs['fill'] == 'location':
            df = df.merge(postal_data, how='left', on='postal_code')

        df.insert(col_loc, kwargs['fill'], df.pop(kwargs["fill"]))

    df.to_csv(txt_path, index=False)

    logger.info(
        f'Transformation done: read {university_id}_select.csv in FILES_DIR, '
        f'processed it and created {university_id}_process.txt in DATASETS_DIR'
    )
