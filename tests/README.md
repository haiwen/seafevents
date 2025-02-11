## Test Step:

* enter test directory
* change the host under the `TESTDB` option in the `db.cnf` file to the host of local db. 
* run command `pytest -sv events` 运行命令`pytest-sv events`

将“db.cnf”文件中“TESTDB”选项下的主机更改为本地数据库的主机。

`Note`: if table struct has been changed then you need rebuild sql file by command `python generate_table_sql.py`, and change the host under the `TESTDB` option to `database`.

`注意：如果表结构已更改，则需要通过命令“python generate_table_sql.py”重建sql文件，并将“TESTDB”选项下的主机更改为“database”。

## db.conf

this is the db config file for test.

if you need rebuild sql file, ensure `SEAHUBDB` and `SEAFEVENTSDB` is right.

如果需要重建sql文件，请确保“SEAHUBDB”和“SEAFEVENTSDB”是正确的。
