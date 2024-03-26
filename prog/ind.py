#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import typing as t
from pathlib import Path


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


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Создать таблицу с информацией о магазинах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS markets (
            market_id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_title TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о продуктах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            market_id INTEGER NOT NULL,
            product_count INTEGER NOT NULL,
            FOREIGN KEY(market_id) REFERENCES markets(market_id)
        )
        """
    )
    conn.close()


def add_worker(database_path: Path, name: str, markets: str, count: int) -> None:
    """
    Добавить продукт в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    cursor.execute(
        """
        SELECT market_id FROM markets WHERE market_title = ?
        """,
        (markets,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO markets (market_title) VALUES (?)
            """,
            (markets,),
        )
        market_id = cursor.lastrowid
    else:
        market_id = row[0]
        
    cursor.execute(
        """
        INSERT INTO products (product_name, market_id, product_count)
        VALUES (?, ?, ?)
        """,
        (name, market_id, count),
    )
    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех работников.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT products.product_name, markets.market_title, products.product_count
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


def select_products(database_path: Path, find_name):
    """
    Выбрать продукт с заданным именем.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute (
        """
        SELECT products.product_name, markets.market_title, products.product_count
        FROM products
        INNER JOIN markets ON markets.market_id = products.market_id
        WHERE products.product_name = ?
        """,
        (find_name, )
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
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db", action="store", required=False, default=str(Path.home() / "products.db"), help="The data file name"
    )

    parser = argparse.ArgumentParser("products")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")

    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser("add", parents=[file_parser], help="Add a new product")
    add.add_argument(
        "-n", "--name", action="store", required=True, help="The product's name"
    )
    add.add_argument("-m", "--market", action="store", required=True, help="The market's name")
    add.add_argument(
        "-c", "--count", action="store", type=int, required=True, help="The count"
    )

    _ = subparsers.add_parser(
        "display", parents=[file_parser], help="Display all products"
    )
    
    select = subparsers.add_parser(
        "select", parents=[file_parser], help="Select the products"
    )
    select.add_argument(
        "--sp",
        action="store",
        required=True,
        help="The required name of market",
    )
    args = parser.parse_args(command_line)

    db_path = Path(args.db)
    create_db(db_path)
    print(args)
    if args.command == "add":
        add_worker(db_path, args.name, args.market, args.count)
        
    elif args.command == "display":
        display_products(select_all(db_path))

    elif args.command == "select":
        display_products(select_products(db_path, args.sp))
        pass


if __name__ == '__main__':
    main()