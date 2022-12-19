from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE Critic("
                     "ID INTEGER PRIMARY KEY,"
                     "Name TEXT NOT NULL,"
                     ");"

                     "CREATE TABLE Movie("
                     "Name TEXT NOT NULL,"
                     "Year INTEGER NOT NULL,"
                     "PRIMARY KEY(Name, Year),"
                     "Genre TEXT NOT NULL,"

                     "CREATE TABLE Actor("
                     "ID INTEGER PRIMARY KEY CHECK (ID > 0),"
                     "Name TEXT NOT NULL,"
                     "Age INTEGER NOT NULL CHECK (Age > 0),"
                     "Height INTEGER NOT NULL CHECK (Height > 0 ),"
                     ");"

                     "CREATE TABLE Studio("
                     "ID INTEGER PRIMARY KEY,"
                     "Name TEXT NOT NULL,"
                     ");"

                     "COMMIT;"
                     )
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Critic;"
                     "DELETE FROM Movie;"
                     "DELETE FROM Actor;"
                     "DELETE FROM Studio;"
                     "COMMIT;"
                     )
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()



def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Critic CASCADE;"
                     "DROP TABLE IF EXISTS Movie CASCADE;"
                     "DROP TABLE IF EXISTS Actor CASCADE;"
                     "DROP TABLE IF EXISTS Studio CASCADE;"
                     "COMMIT;"
                     )
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    # TODO: implement
    pass


def deleteCritic(critic_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getCriticProfile(critic_id: int) -> Critic:
    # TODO: implement
    pass


def addActor(actor: Actor) -> ReturnValue:
    # TODO: implement
    pass


def deleteActor(actor_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getActorProfile(actor_id: int) -> Actor:
    # TODO: implement
    pass


def addMovie(movie: Movie) -> ReturnValue:
    # TODO: implement
    pass


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    # TODO: implement
    pass


def getMovieProfile(movie_name: str, year: int) -> Movie:
    # TODO: implement
    pass


def addStudio(studio: Studio) -> ReturnValue:
    # TODO: implement
    pass


def deleteStudio(studio_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getStudioProfile(studio_id: int) -> Studio:
    # TODO: implement
    pass


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    # TODO: implement
    pass


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: implement
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    # TODO: implement
    pass


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass

# GOOD LUCK!
