#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import typing as t

import psycopg2


def display_products(staff: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список продуктов
    """
    if staff:
        line = "+-{}-+-{}-+-{}-+-{}-+".format(
            "-" * 4, "-" * 30, "-" * 20, "-" * 10
        )
        print(line)
        print(
            "| {:^4} | {:^30} | {:^20} | {:^10} |".format(
                "№", "Название продукта", "Имя магазина", "Стоимость"
            )
        )
        print(line)

        for idx, worker in enumerate(staff, 1):
            print(
                "| {:>4} | {:<30} | {:<20} | {:>8} |".format(
                    idx,
                    worker.get("name", ""),
                    worker.get("market", ""),
                    worker.get("count", 0),
                )
            )
            print(line)
    else:
        print("Список работников пуст.")


def create_db() -> None:
    """
    Создать базу данных.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="pwsi6tg3",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()
    # Создать таблицу с информацией о магазинах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS markets (
            market_id serial primary key,
            market_title TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о продуктах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id serial primary key,
            product_name TEXT NOT NULL,
            market_id INTEGER NOT NULL,
            product_count INTEGER NOT NULL,
            FOREIGN KEY(market_id) REFERENCES markets(market_id)
        )
        """
    )
    conn.commit()
    conn.close()


def add_worker(name: str, markets: str, count: int) -> None:
    """
    Добавить продукт в базу данных.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="pwsi6tg3",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT market_id FROM markets WHERE market_title = %s
        """,
        (markets,),
    )
    market_id = cursor.lastrowid
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO markets (market_title) VALUES (%s)
            """,
            (markets,),
        )
        market_id = cursor.lastrowid
    else:
        market_id = row[0]
    conn.commit()
    cursor.execute(
        """
        insert into products (product_name, market_id, product_count) 
        values (%s, %s, %s);
        """,
        (name, market_id, count),
    )
    conn.commit()
    conn.close()


def select_all() -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех работников.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="pwsi6tg3",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 
        products.product_name, 
        markets.market_title, 
        products.product_count
        FROM products
        INNER JOIN markets ON markets.market_id = products.market_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "market": row[1],
            "count": row[2],
        }
        for row in rows
    ]


def select_products(find_name):
    """
    Выбрать продукт с заданным именем.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="pwsi6tg3",
        host="localhost",
        port=5432,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT products.product_name, markets.market_title, products.product_count
        FROM products
        INNER JOIN markets ON markets.market_id = products.market_id
        WHERE products.product_name = %s
        """,
        (find_name,),
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
            "name": row[0],
            "market": row[1],
            "count": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    parser = argparse.ArgumentParser("products")
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser("add", help="Add a new product")
    add.add_argument(
        "-n",
        "--name",
        action="store",
        required=True,
        help="The product's name",
    )
    add.add_argument(
        "-m",
        "--market",
        action="store",
        required=True,
        help="The market's name",
    )
    add.add_argument(
        "-c",
        "--count",
        action="store",
        type=int,
        required=True,
        help="The count",
    )

    _ = subparsers.add_parser("display", help="Display all products")

    select = subparsers.add_parser("select", help="Select the products")
    select.add_argument(
        "--sp",
        action="store",
        required=True,
        help="The required name of market",
    )
    args = parser.parse_args(command_line)
    create_db()
    if args.command == "add":
        add_worker(args.name, args.market, args.count)

    elif args.command == "display":
        display_products(select_all())

    elif args.command == "select":
        display_products(select_products(args.sp))
        pass


if __name__ == "__main__":
    main()
