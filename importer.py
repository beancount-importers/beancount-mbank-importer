#!/usr/bin/env python3

from io import StringIO
from datetime import timedelta
from dateutil.parser import parse

import pandas as pd
import chardet
from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
import logging


def get_narration(row, title_col="Tytuł", description_col="Opis operacji"):
    if pd.isnull(row[title_col]):
        return (row[description_col], None)
    title = row[title_col]
    title = title.split("DATA TRANSAKCJI")[0].strip()
    location = get_location(title)
    if location:
        title = title.replace(f"/{location}", "").strip()
        location = location.title()
    return (title, location)


def get_location(
    title: str,
    shit_locations=[
        "POZNAN",
        "LONDON",
        "GBR",
        "VILNIUS",
        "INTERNET",
        "AMSTERDAM",
        "BOURNEMOUT",
        "DUBLIN",
        "HTTPSABSOL",
        "O",
        "RFB",
        "UD",
        "SZEMUD",
    ],
):
    parts = title.split("/")
    if len(parts) == 1:
        return None
    if parts[1].upper() in shit_locations or not parts[1].isalpha():
        return None
    return parts[1]


class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Revolut CSV files."""

    def __init__(self, regexps, account, currency):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.currency = currency

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries):
        entries = []
        # print(dir(file))
        # print(file.name)
        # print(file.head())
        # print(file.contents())

        with open(file.name, "rb") as f:
            raw_bytes = f.read()

        enc = chardet.detect(raw_bytes)  # enc['encoding'] == 'Windows-1252'
        csv = raw_bytes.decode(enc["encoding"], "replace")  # replace incorrect chars
        csv = [  # get rid of metadata from header and footer
            x
            for x in csv.split("\n")
            if (x.startswith("20") and len(x) > 12)  # row of data
            or x.startswith("#Data operacji")  # csv header
        ]
        csv = "\n".join(csv)
        csv = csv.translate(
            {
                ord("¥"): "Ą",
                ord("¹"): "ą",
                ord("Ê"): "Ę",
                ord("ê"): "ę",
                ord("Æ"): "Ć",
                ord("Ê"): "ć",
                ord("£"): "Ł",
                ord("³"): "ł",
                ord("Ñ"): "Ń",
                ord("ń"): "ń",
                ord("Ó"): "Ó",
                ord("ó"): "ó",
                ord("Œ"): "Ś",
                ord("œ"): "ś",
                ord("¯"): "Ż",
                ord("ż"): "ż",
                ord("Ź"): "Ź",
                ord("ź"): "ż",
            }
        )
        df = pd.read_csv(StringIO(csv), sep=";")
        df.drop(columns=["Unnamed: 8"], inplace=True)
        df.columns = [c.replace("#", "") for c in df.columns]
        for idx, row in df.iterrows():
            metakv = {}
            try:
                bal = D(
                    row["Saldo po operacji"].replace(" ", "").replace(",", ".").strip()
                )
                amount_raw = D(row["Kwota"].replace(" ", "").replace(",", ".").strip())
                amt = amount.Amount(amount_raw, self.currency)
                balance = amount.Amount(bal, self.currency)
                book_date = parse(row["Data księgowania"].strip()).date()
                narration, location = get_narration(row)
            except Exception as e:
                logging.warning(e)
                continue
            if location:
                metakv["location"] = location
            metakv["description"] = row["Opis operacji"]
            meta = data.new_metadata(file.name, 0, metakv)
            entry = data.Transaction(
                meta=meta,
                date=book_date,
                flag="*",
                payee="",
                narration=narration,
                tags=data.EMPTY_SET,
                links=data.EMPTY_SET,
                postings=[
                    data.Posting(self.account, -amt, None, None, None, None),
                    data.Posting("Expenses:FIXME", amt, None, None, None, None),
                ],
            )
            entries.append(entry)
        return entries
