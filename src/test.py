import psycopg2
import pandas as pd
import numpy as np
conn = psycopg2.connect(database="test",
                        user="quocthanh",
                        password="quocthanh1804",
                        host="172.26.115.201"
                        )
test = pd.read_csv("../staging/transformed/players.csv", dtype={"IsRetired": bool, "ToWildrift": bool})
test["Age"] = test["Age"].astype("Int64")
test = test.replace([pd.NaT], [None])
print(test["AllName"])
cur = conn.cursor()



for row in test.itertuples(index=False):
    players_table = (row.Player, row.Country, row.Age, row.ResidencyFormer, row.Team, row.Residency, row.Role, row.RoleLast, row.IsRetired, row.ToWildrift)
    cur.execute("""INSERT INTO players VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", players_table)
    if row.FavChamps is not None:
        for champ in row.FavChamps.split(","):
            favchamps_table = (row.Player, champ)
            cur.execute("""INSERT INTO playersfavchamps VALUES (%s, %s)""", favchamps_table)
    if row.AllName is not None:
        for name in row.AllName.split(", "):
            allname_table = (row.Player, name)
            cur.execute("""INSERT INTO playerspronames VALUES (%s, %s)""", allname_table)
    conn.commit()
cur.close()
conn.close()    