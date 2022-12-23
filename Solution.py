from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor

DEBUG = False

# ---------------------------------- CRUD API: ----------------------------------


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()

        create_critic_table = """
                     CREATE TABLE IF NOT EXISTS Critic(
                     ID INTEGER NOT NULL PRIMARY KEY,
                     Name TEXT NOT NULL
                     );
                     """

        create_movie_table = """
                        CREATE TABLE IF NOT EXISTS Movie(
                        Name TEXT NOT NULL,
                        Year INTEGER NOT NULL CHECK (Year > 1984),
                        PRIMARY KEY(Name, Year),
                        Genre TEXT NOT NULL
                        );
                        """

        create_actor_table = """
                        CREATE TABLE IF NOT EXISTS Actor(
                        ID INTEGER PRIMARY KEY CHECK (ID > 0),
                        Name TEXT NOT NULL,
                        Age INTEGER NOT NULL CHECK (Age > 0),
                        Height INTEGER NOT NULL CHECK (Height > 0 )
                        );
                        """

        create_studio_table = """
                        CREATE TABLE IF NOT EXISTS Studio(
                        ID INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                        );
                        """

        # more...

        conn.execute(create_critic_table)
        conn.execute(create_movie_table)
        conn.execute(create_actor_table)
        conn.execute(create_studio_table)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Critic;"
                     "DELETE FROM Movie;"
                     "DELETE FROM Actor;"
                     "DELETE FROM Studio;"
                     )
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
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
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None

    critic_id = critic.getCriticID()
    critic_id = critic_id if critic_id is not None else "NULL"
    critic_name = critic.getName()
    critic_name = stringQouteMark(critic_name)

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Critic (ID, Name) VALUES ({critic_id}, {critic_name});"
        query = query.format(critic_id=critic_id, critic_name=critic_name)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteCritic(critic_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getCriticProfile(critic_id: int) -> Critic:
    # TODO: implement
    pass


def addActor(actor: Actor) -> ReturnValue:
    conn = None

    actor_id = actor.getActorID()
    actor_id = actor_id if actor_id is not None else "NULL"

    actor_name = actor.getActorName()


    actor_age = actor.getAge()
    actor_age = actor_age if actor_age is not None else "NULL"

    actor_height = actor.getHeight()
    actor_height = actor_height if actor_height is not None else "NULL"

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Actor (ID, Name, Age, Height) VALUES ({actor_id}, {actor_name}, {actor_age}, {actor_height});"
        actor_name = stringQouteMark(actor_name)
        query = query.format(actor_id=actor_id,
                             actor_name=actor_name,
                             actor_age=actor_age,
                             actor_height=actor_height)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    actor = Actor
    actor = actor.getActorProfile(actor_id)

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Actor WHERE id={0}").format(
            sql.Literal(actor_id))
        rows_effected, _ = conn.execute(query)

#        query = query.format(actor_id=actor_id, actor_name=actor_name,
#                             actor_age=actor_age, actor_height=actor_height)
 #       conn.execute(query)
  #      conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
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
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


pass


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    rows_effected, result = 0, Actor()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(
            "SELECT * FROM Actor", printSchema=actor_id)
        # rows_effected is the number of rows received by the SELECT
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
        return result


def addMovie(movie: Movie) -> ReturnValue:
    conn = None

    movie_name = movie.getMovieName()
    movie_name = stringQouteMark(movie_name)
    movie_year = movie.getYear()
    movie_year = movie_year if movie_year is not None else "NULL"
    movie_genre = movie.getGenre()
    movie_genre = stringQouteMark(movie_genre)
    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Movie (Name, Year, Genre) VALUES ({movie_name}, {movie_year}, {movie_genre});"
        query = query.format(movie_name=movie_name,
                             movie_year=movie_year,
                             movie_genre=movie_genre)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    # TODO: implement
    pass


def getMovieProfile(movie_name: str, year: int) -> Movie:
    # TODO: implement
    pass


def addStudio(studio: Studio) -> ReturnValue:
    conn = None

    studio_id = studio.getStudioID()
    studio_id = studio_id if studio_id is not None else "NULL"
    studio_name = studio.getStudioName()
    studio_name = stringQouteMark(studio_name)

    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Studio (ID, Name) VALUES ({studio_id}, {studio_name});"
        query = query.format(studio_id=studio_id,
                             studio_name=studio_name)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


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

def stringQouteMark(string: str) -> str:
    if not string:
        return "Null"

    return "'" + string + "'"

# GOOD LUCK!
