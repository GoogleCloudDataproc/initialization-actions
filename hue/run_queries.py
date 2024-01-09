"""run_queries.py: Script to run sample hive sql queries.

This script is fed to hue shell by verify_hue_running.py.
"""

from beeswax.server import dbms
from django.contrib.auth.models import User


def execute_and_wait(database, ms, query):
    # Bump up the timeout from 30 seconds (the default) to 2 minutes.
    return database.execute_and_wait(
        ms.hql_query(query), timeout_sec=120.0, sleep_interval=1.0)


hue, created = User.objects.get_or_create(username='hue')
db = dbms.get(hue)
execute_and_wait(db, dbms, 'CREATE TABLE store (food string, price int)')
execute_and_wait(db, dbms,
                 "INSERT INTO TABLE store VALUES ('a', 1), ('b', 2), ('c', 3)")
query_handle = execute_and_wait(db, dbms,
                                'SELECT food FROM store WHERE price < 3')
result = db.fetch(query_handle)
for row in result.rows():
    # pylint: disable=superfluous-parens
    print(row)
