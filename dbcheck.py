import sqlite3

DB_FILE_PATH = "dbu_db.sqlite"
DB_TABLE = "DBU_DAILY_AGGR_VIEW"

with sqlite3.connect(DB_FILE_PATH) as conn:
    cur = conn.cursor()

    # cur.execute('.schema ' + DB_TABLE)
    # print(cur.fetchone())

    cur.execute('SELECT * FROM ' + DB_TABLE)
    rows = cur.fetchall()
    names = list(map(lambda x: x[0], cur.description))
    print(names)
    print(len(rows))
    rows = rows[:10]

    for row in rows:
        print(row)