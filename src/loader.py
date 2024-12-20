import pandas as pd
import psycopg2
from airflow.models import Variable
from io import BytesIO
def teams_loader(**kwargs):
    # cur = db_con.cursor()
    cur = kwargs.get("db_con").cursor()
    s3 = kwargs.get("s3")
    obj = s3.get_key("transformed/teams.csv", "leaguepedia")
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    # teams = pd.read_csv("../staging/transformed/teams.csv")
    teams = pd.read_csv(read_buffer)
    print(teams)
    for row in teams.itertuples(index=None,name=None):
        cur.execute("INSERT INTO teams VALUES (%s, %s, %s, %s, %s, %s)", row)
    cur.close()
    kwargs.get("db_con").commit()
    return

def players_loader(**kwargs):
    # cur = db_con.cursor()
    cur = kwargs.get("db_con").cursor()
    s3 = kwargs.get("s3")
    obj = s3.get_key("transformed/players.csv", "leaguepedia")
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    # players = pd.read_csv("../staging/transformed/players.csv", keep_default_na=False)
    players = pd.read_csv(read_buffer, keep_default_na=False)
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
    cur.close()
    print(players)
    kwargs.get("db_con").commit()
    return 

def tournaments_loader(**kwargs):
    # cur = db_con.cursor()
    cur = kwargs.get("db_con").cursor()
    s3 = kwargs.get("s3")
    obj = s3.get_key("transformed/tournaments.csv", "leaguepedia")
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    # tournaments = pd.read_csv("../staging/transformed/tournaments.csv", parse_dates=["DateStart", "Date"])
    tournaments = pd.read_csv(read_buffer, parse_dates=["DateStart", "Date"])
    tournaments = tournaments.replace([pd.NaT], [None])
    print(tournaments.dtypes)
    for row in tournaments.itertuples(index=None):
        parameters = (row.OverviewPage, row.DateStart, row.Date, row.Region, row.Country, row.EventType, row.League)
        print(parameters)
        cur.execute("INSERT INTO tournaments VALUES (%s, %s, %s, %s, %s, %s, %s)", parameters)
    cur.close()
    print(tournaments)
    kwargs.get("db_con").commit()

    return 

def tournamentresults_loader(**kwargs):
    # cur = db_con.cursor()
    cur = kwargs.get("db_con").cursor()
    s3 = kwargs.get("s3")
    obj = s3.get_key("transformed/tournamentresults.csv", "leaguepedia")
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    # tournamentresults = pd.read_csv("../staging/transformed/tournamentresults.csv", names=["OverviewPage", "Prize_USD", "Place", "Team"], skiprows=[0]).convert_dtypes()
    tournamentresults = pd.read_csv(read_buffer, names=["OverviewPage", "Prize_USD", "Place", "Team"], skiprows=[0]).convert_dtypes()
    tournamentresults = tournamentresults.replace([pd.NaT], [None])
    # tournamentresults["Prize_USD"] = tournamentresults["Prize_USD"].astype(float)
    for row in tournamentresults.itertuples(index=None):
        parameters = (row.OverviewPage, row.Prize_USD, row.Place, row.Team)
        print(parameters)
        cur.execute("INSERT INTO tournamentresults VALUES (%s, %s, %s, %s)", parameters)
    cur.close()
    kwargs.get("db_con").commit()
    print(tournamentresults)
    return

def scoreboardgames_loader(**kwargs):
    # cur = db_con.cursor()
    cur = kwargs.get("db_con").cursor()
    s3 = kwargs.get("s3")
    obj = s3.get_key("transformed/scoreboardgames.csv", "leaguepedia")
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    # scoreboardgames = pd.read_csv("../staging/transformed/scoreboardgames.csv").convert_dtypes()
    scoreboardgames = pd.read_csv(read_buffer).convert_dtypes()
    scoreboardgames = scoreboardgames.rename(columns={"GameID": "gameid", "OverviewPage": "overviewpage", "Team1": "team1", "Team2": "team2", "WinTeam": "winteam", "DateTime UTC": "timestamp", "Gamelength": "gamelength"})
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
    cur.close()
    kwargs.get("db_con").commit()
    print(scoreboardgames)
    return
def main():
    conn = psycopg2.connect(database=Variable.get("ETL_dbname"),
                        user=Variable.get("user"),
                        password=Variable.get("password"),
                        host=Variable.get("host")
                        )
    
    teams_loader(conn)
    players_loader(conn)
    tournaments_loader(conn)
    tournamentresults_loader(conn)
    scoreboardgames_loader(conn)

    conn.close()


