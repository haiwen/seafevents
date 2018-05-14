## Test Step:

* enter test directory
* change the host under the `TESTDB` option in the `db.cnf` file to the host of local db.
* run command `pytest -sv events`

`Note`: if table struct has been changed then you need rebuild sql file by command `python generate_table_sql.py`, and change the host under the `TESTDB` option to `database`.

## db.conf

this is the db config file for test.

if you need rebuild sql file, ensure `SEAHUBDB` and `SEAFEVENTSDB` is right.
