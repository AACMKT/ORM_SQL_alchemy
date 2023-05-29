from models import *
from sqlalchemy.orm import sessionmaker

file_path = 'tests_data.json'

'''Создаем сессию'''

DSN = 'postgresql://postgres:PASSWORD@localhost:5432/DBASE_NAME'
engine = sq.create_engine(DSN)

'''Ниже - закомментированные функции в порядке их выполнения:'''

# create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()


# fill_tables_with_json(file_path, session)

# request(session)

session.close()

# drop_tables(engine)
