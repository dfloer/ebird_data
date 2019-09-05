import models
from sqlalchemy import create_engine
import argparse

def create_tables(connection_url):
    """
    Simple convenince function that creates the tables specified in the models file.
    Args:
        connection_url (string): Connection URL to use to connect to the database. See https://docs.sqlalchemy.org/en/13/core/engines.html for how to form this string.
    """
    engine = create_engine(connection_url, echo=True)
    models.Base.metadata.create_all(engine)

def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--connection', dest="connection_url", help="SQLAlchemy connection URL.", metavar="URL", required=True)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    options = parse_command_line()
    connection = options.connection_url
    create_tables(connection)