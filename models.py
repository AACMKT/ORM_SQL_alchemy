import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship
import json
import pandas as pd
from tabulate import tabulate

'''Создаем модели классов'''


Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=64), unique=True)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=48), unique=True)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="books")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=64), unique=True)


class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref="books_on_stock")
    shop = relationship(Shop, backref="shops")


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer)

    stock = relationship(Stock, backref="on_sale")


'''Определяем функцию для создания таблиц'''


def create_tables(engine):
    Base.metadata.create_all(engine)


'''Определяем функцию для удаления таблиц'''


def drop_tables(engine):
    Base.metadata.drop_all(engine)


def fill_tables_with_json(file_path, session):

    try:

        with open(file_path, 'r', encoding='utf-8') as tables_content:
            data = json.load(tables_content)
            for item in data:
                if item['model'] == "publisher":
                    session.add(Publisher(id=item["pk"], name=item["fields"]["name"]))
                    session.commit()
                elif item['model'] == "book":
                    session.add(Book(id=item["pk"], title=item["fields"]["title"],
                                     id_publisher=item["fields"]["id_publisher"]))
                    session.commit()
                elif item['model'] == "shop":
                    session.add(Shop(id=item["pk"], name=item["fields"]["name"]))
                    session.commit()
                elif item['model'] == "stock":
                    session.add(Stock(id=item["pk"], id_book=item["fields"]["id_book"],
                                      id_shop=item["fields"]["id_shop"], count=item["fields"]["count"]))
                    session.commit()
                elif item['model'] == "sale":
                    session.add(Sale(id=item["pk"], price=item["fields"]["price"],
                                     date_sale=item["fields"]["date_sale"],
                                     count=item["fields"]["count"], id_stock=item["fields"]["id_stock"]))
                    session.commit()

    except Exception:
        print('Something went wrong. Probably tables already filled in with same data.')


'''Функция запроса данных из таблиц'''


def request(session):

    exists_publ_id = []
    p_id = session.query(Publisher)
    for el in p_id:
        exists_publ_id += [int(el.id)]

    while True:
        try:
            publ_id = int(input('Введите ID издателя: '))
            if publ_id in exists_publ_id:
                query_stock = session.query(Stock).join(Book.books_on_stock).filter(Book.id_publisher == publ_id)\
                    .subquery('filtered')
                sold = session.query(Sale).join(query_stock, Sale.id_stock == query_stock.c.id)

                result = {'book_name': [], 'shop': [], 'price': [], 'purchase date': []}
                for i in sold.all():
                    result['book_name'].append(i.stock.book.title)
                    result['shop'].append(i.stock.shop.name)
                    result['price'].append(i.price)
                    result['purchase date'].append(i.date_sale)
                ans = pd.DataFrame(result, index=None)
                print(tabulate(ans, headers='keys', tablefmt='psql', stralign='center',  showindex=False))
                break
            else:
                raise Exception
        except Exception:
            print('Некорректное значение! Попробуйте снова')
