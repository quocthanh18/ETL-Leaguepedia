import psycopg2
from airflow.models import Variable


def create_players_tables(db_con):
    cur = db_con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS players(
                player character varying PRIMARY KEY NOT NULL,
                country character varying,
                birthdate date,
                residencyformer character varying,
                team character varying,
                residency character varying,
                role character varying,
                rolelast character varying,
                isretired boolean,
                towildrift boolean)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS playersfavchamps(
                player character varying,
                champ character varying)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS playerspronames(
                player character varying,
                allname character varying)""")
    
    cur.execute("""ALTER TABLE playersfavchamps
                ADD CONSTRAINT fk_player
                FOREIGN KEY (player)
                REFERENCES players(player)
                ON DELETE CASCADE""")


    cur.execute("""ALTER TABLE playerspronames
                ADD CONSTRAINT fk_player
                FOREIGN KEY (player)
                REFERENCES players(player)
                ON DELETE CASCADE""")
    
    db_con.commit()
    cur.close()
    return

def create_tournaments_table(db_con):
    cur = db_con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS tournaments(
                overviewpage character varying PRIMARY KEY NOT NULL,
                datestart date,
                dateend date,
                region character varying,
                country character varying,
                eventtype character varying,
                league character varying)""")
    db_con.commit()
    cur.close()
    return

def create_tournamentresults_table(db_con):
    cur = db_con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS tournamentresults(
                overviewpage character varying,
                prize_usd real,
                place character varying,
                team character varying)""")
    
    cur.execute("""ALTER TABLE tournamentresults
                ADD CONSTRAINT fk_overviewpage
                FOREIGN KEY (overviewpage)
                REFERENCES tournaments(overviewpage)""")
    db_con.commit()
    cur.close()
    return

def create_scoreboardgames_table(db_con):
    cur = db_con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS scoreboardgames(
                overviewpage character varying,
                team1 character varying,
                team2 character varying,
                winteam character varying,
                timestamp date,
                gamelength character varying,
                gameid character varying NOT NULL PRIMARY KEY)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS gamebans(
                gameid character varying,
                team character varying,
                champ character varying,
                pick_order smallint,

                FOREIGN KEY (gameid)
                REFERENCES scoreboardgames(gameid))""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS gamepicks(
                gameid character varying,
                team character varying,
                champ character varying,
                role character varying,

                FOREIGN KEY (gameid)
                REFERENCES scoreboardgames(gameid))""")
    db_con.commit()
    cur.close()
    return

def create_teams_table(db_con):
    cur = db_con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS teams(
                name character varying PRIMARY KEY NOT NULL,
                short character varying,
                location character varying,
                region character varying,
                isdisbanded boolean,
                renamedto character varying)""")
    db_con.commit()
    cur.close()
    return
def main():
    conn = psycopg2.connect(database=Variable.get("ETL_dbname"),
                        user=Variable.get("user"),
                        password=Variable.get("password"),
                        host=Variable.get("host")
                        )
    
    create_players_tables(conn)

    create_tournaments_table(conn)

    create_tournamentresults_table(conn)

    create_scoreboardgames_table(conn)

    create_teams_table(conn)
