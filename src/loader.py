import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
def teams_loader(db_con):
    cur = db_con.cursor()
    teams = pd.read_csv("../staging/transformed/teams.csv")
    for row in teams.itertuples(index=None,name=None):
        cur.execute("INSERT INTO teams VALUES (%s, %s, %s, %s, %s, %s)", row)
    db_con.commit()
    cur.close()
    return

def players_loader(db_con):
    cur = db_con.cursor()
    players = pd.read_csv("../staging/transformed/players.csv", keep_default_na=False)
    players = players.replace("", None)
    for row in players.itertuples(index=None):
        parameters = (row.Player, row.Country, row.ResidencyFormer, row.Team, row.Residency, row.Role, row.RoleLast, row.IsRetired, row.ToWildrift, row.Birthdate)
        cur.execute("""
                    INSERT INTO players (player, country, residencyformer, team, residency, role, rolelast, isretired, towildrift, birthdate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, parameters)
        if row.FavChamps is not None:
            for champ in row.FavChamps.split(","):
                favchamps_table = (row.Player, champ)
                cur.execute("""INSERT INTO playersfavchamps VALUES (%s, %s)""", favchamps_table)

        if row.AllName is not None:
            for name in row.AllName.split(", "):
                allname_table = (row.Player, name)
                cur.execute("""INSERT INTO playerspronames VALUES (%s, %s)""", allname_table)

    db_con.commit()
    cur.close()
    return 

def tournaments_loader(db_con):
    cur = db_con.cursor()
    tournaments = pd.read_csv("../staging/transformed/tournaments.csv", parse_dates=["DateStart", "Date"])
    tournaments = tournaments.replace([pd.NaT], [None])
    print(tournaments.dtypes)
    for row in tournaments.itertuples(index=None):
        parameters = (row.OverviewPage, row.DateStart, row.Date, row.Region, row.Country, row.EventType, row.League)
        print(parameters)
        cur.execute("INSERT INTO tournaments VALUES (%s, %s, %s, %s, %s, %s, %s)", parameters)
    db_con.commit()
    cur.close()
    return 

def tournamentresults_loader(db_con):
    cur = db_con.cursor()
    tournamentresults = pd.read_csv("../staging/transformed/tournamentresults.csv", names=["OverviewPage", "Prize_USD", "Place", "Team"], skiprows=[0]).convert_dtypes()
    tournamentresults = tournamentresults.replace([pd.NaT], [None])
    # tournamentresults["Prize_USD"] = tournamentresults["Prize_USD"].astype(float)
    print(tournamentresults["Prize_USD"])
    for row in tournamentresults.itertuples(index=None):
        parameters = (row.OverviewPage, row.Prize_USD, row.Place, row.Team)
        cur.execute("INSERT INTO tournamentresults VALUES (%s, %s, %s, %s)", parameters)
    db_con.commit()
    cur.close()
    return

def scoreboardgames_loader(db_con):
    cur = db_con.cursor()
    scoreboardgames = pd.read_csv("../staging/transformed/scoreboardgames.csv").convert_dtypes()
    scoreboardgames = scoreboardgames.rename(columns={"GameID": "gameid", "OverviewPage": "overviewpage", "Team1": "team1", "Team2": "team2", "WinTeam": "winteam", "DateTime UTC": "timestamp", "Gamelength": "gamelength"})
    print(scoreboardgames.columns)
    for row in scoreboardgames.itertuples(index=None):
        parameters = (row.overviewpage, row.team1, row.team2, row.winteam, row.timestamp, row.gamelength, row.GameId)        
        cur.execute("INSERT INTO scoreboardgames VALUES (%s, %s, %s, %s, %s, %s, %s)", parameters)

        for idx, ban in enumerate(row.Team1Bans.split(",")):
            ban_table = (row.GameId, row.team1, ban, idx)
            cur.execute("INSERT INTO gamebans VALUES (%s, %s, %s, %s)", ban_table)

        for idx, ban in enumerate(row.Team2Bans.split(",")):
            ban_table = (row.GameId, row.team2, ban, idx)
            cur.execute("INSERT INTO gamebans VALUES (%s, %s, %s, %s)", ban_table)

        team1picks = row.Team1Picks.split(",")
        team2picks = row.Team2Picks.split(",")
        for idx in range(5):
            role = ["Top", "Jungle", "Mid", "Bot", "Support"][idx]
            team1_picks = (row.GameId, row.team1, team1picks[idx], role)
            team2_picks = (row.GameId, row.team2, team2picks[idx], role)
            cur.execute("INSERT INTO gamepicks VALUES (%s, %s, %s, %s)", team1_picks)
            cur.execute("INSERT INTO gamepicks VALUES (%s, %s, %s, %s)", team2_picks)
    db_con.commit()
    return
def main():
    conn = psycopg2.connect(database=os.getenv("dbname"),
                        user=os.getenv("user"),
                        password=os.getenv("password"),
                        host=os.getenv("host")
                        )
    
    # teams_loader(conn)
    # players_loader(conn)
    # tournaments_loader(conn)
    # tournamentresults_loader(conn)
    scoreboardgames_loader(conn)

    conn.close()



if __name__ == "__main__":
    main()


