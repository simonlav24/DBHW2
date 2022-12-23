from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor

DEBUG = True

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

        create_Ratings_table = """
                        CREATE TABLE IF NOT EXISTS Ratings(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        CriticID INTEGER NOT NULL REFERENCES Critic(ID) ON DELETE CASCADE,
                        Rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <=5),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year) ON DELETE CASCADE,
                        UNIQUE(MovieName, MovieYear, CriticID)
                        );
                        """

        # more...

        conn.execute(create_critic_table)
        conn.execute(create_movie_table)
        conn.execute(create_actor_table)
        conn.execute(create_studio_table)
        conn.execute(create_Ratings_table)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except Exception as e:
        if DEBUG:
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
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except Exception as e:
        if DEBUG:
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
                     "DROP TABLE IF EXISTS Ratings CASCADE;"
                     "COMMIT;"
                     )
    except DatabaseException.ConnectionInvalid as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
        conn.rollback()
    except Exception as e:
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None

    critic_id = critic.getCriticID()
    critic_id = validateInteger(critic_id)
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
        result = ReturnValue.ERROR
        if DEBUG:
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
        if DEBUG:
            print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    
    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "DELETE FROM Critic Where ID = {critic_id};"
        query = query.format(critic_id=critic_id)
        rows_count, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if rows_count == 0:
            result = ReturnValue.NOT_EXISTS
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()
    
    return result


def getCriticProfile(critic_id: int) -> Critic:
    conn = None

    critic_id = validateInteger(critic_id)
    result = Critic.badCritic()

    rows_count = 0
    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Critic Where ID = {critic_id};"
        query = query.format(critic_id=critic_id)
        rows_count, rows = conn.execute(query)


    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()

        if rows_count == 1:
            row = rows[0]
            res_critic_id = row["id"]
            res_critic_name = row["name"]
            result = Critic(res_critic_id, res_critic_name)

        return result


def addActor(actor: Actor) -> ReturnValue:
    conn = None

    actor_id = validateInteger(actor.getActorID())
    actor_name = stringQouteMark(actor.getActorName())
    actor_age = validateInteger(actor.getAge())
    actor_height = validateInteger(actor.getHeight())

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Actor (ID, Name, Age, Height) VALUES ({actor_id}, {actor_name}, {actor_age}, {actor_height});"
        query = query.format(actor_id=actor_id,
                             actor_name=actor_name,
                             actor_age=actor_age,
                             actor_height=actor_height)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
        if DEBUG:
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
        if DEBUG:
            print(e)
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "DELETE FROM Actor Where (ID = {actor_id});"
        query = query.format(actor_id=actor_id)
        rows_count, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if rows_count == 0:
            result = ReturnValue.NOT_EXISTS
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()
    return result



def getActorProfile(actor_id: int) -> Actor:
    conn = None

    result = Actor.badActor()
    rows_count = 0

    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Actor Where (ID = {actor_id});"
        query = query.format(actor_id=actor_id)
        rows_count, rows = conn.execute(query)

    except Exception as e:
        if DEBUG:
            print(e)
    finally:
        if rows_count == 1:
            row = rows[0]
            res_actor_id = row["id"]
            res_actor_name = row["name"]
            res_actor_age = row["age"]
            res_actor_height = row["height"]
            result = Actor(res_actor_id, res_actor_name, res_actor_age, res_actor_height)
        if conn is not None:
            conn.close()
    return result


def addMovie(movie: Movie) -> ReturnValue:
    conn = None

    movie_name = stringQouteMark(movie.getMovieName())
    movie_year = validateInteger(movie.getYear())
    movie_genre = stringQouteMark(movie.getGenre())
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
        result = ReturnValue.ERROR
        if DEBUG:
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
        if DEBUG:
            print(e)
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None

    result = ReturnValue.OK

    string_movie_name = stringQouteMark(movie_name)
    try:
        conn = Connector.DBConnector()
        query = "DELETE FROM Movie Where (Name = {string_movie_name} AND Year = {movie_year});"
        query = query.format(string_movie_name=string_movie_name,
                             movie_year=year)
        rows_count, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if rows_count == 0:
            result = ReturnValue.NOT_EXISTS
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None

    string_movie_name = stringQouteMark(movie_name)
    movie_year = validateInteger(year)
    result = Movie.badMovie()

    rows_count = 0
    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Movie Where (Name = {string_movie_name} AND Year = {movie_year});"
        query = query.format(string_movie_name=string_movie_name,
                             movie_year=movie_year)
        rows_count, rows = conn.execute(query)

    except Exception as e:
        if DEBUG:
            print(e)
    finally:
        if rows_count == 1:
            row = rows[0]
            res_movie_name = row["name"]
            res_movie_year = row["year"]
            res_movie_genre = row["genre"]
            result = Movie(res_movie_name, res_movie_year, res_movie_genre)
    
    if conn is not None:
        conn.close()
    return result



def addStudio(studio: Studio) -> ReturnValue:
    conn = None

    studio_id = validateInteger(studio.getStudioID())
    studio_name = stringQouteMark(studio.getStudioName())

    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Studio (ID, Name) VALUES ({studio_id}, {studio_name});"
        query = query.format(studio_id=studio_id,
                             studio_name=studio_name)
        conn.execute(query)
        conn.commit()
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
        if DEBUG:
            print(e)
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()
    return result


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None

    result = ReturnValue.OK

    try:
        conn = Connector.DBConnector()
        query = "DELETE FROM Studio Where (ID = {studio_id});"
        query = query.format(studio_id=studio_id)
        rows_count, _ = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if rows_count == 0:
            result = ReturnValue.NOT_EXISTS
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()
    return result


def getStudioProfile(studio_id: int) -> Studio:
    conn = None

    result = Studio.badStudio()
    rows_count = 0

    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Studio Where (ID = {studio_id});"
        query = query.format(studio_id=studio_id)
        rows_count, rows = conn.execute(query)

    except Exception as e:
        if DEBUG:
            print(e)
    finally:
        if rows_count == 1:
            row = rows[0]
            res_studio_id = row["id"]
            res_studio_name = row["name"]
            result = Studio(res_studio_id, res_studio_name)
        if conn is not None:
            conn.close()
    return result


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None

    result = ReturnValue.OK

    if getCriticProfile(criticID) == Critic.badCritic():
        result = ReturnValue.NOT_EXISTS
        return result
    if getMovieProfile(movieName, movieYear).is_bad():
        result = ReturnValue.NOT_EXISTS
        return result
    if not rating:
        result = ReturnValue.BAD_PARAMS
        return result

    string_movie_name = stringQouteMark(movieName)

    try:
        conn = Connector.DBConnector()
        query = "INSERT INTO Ratings (MovieName, MovieYear, CriticID, rating) VALUES \
                                    ({movieName}, {movieYear}, {criticID}, {rating});"
        query = query.format(movieName=string_movie_name,
                             movieYear=movieYear,
                             criticID=criticID,
                             rating=rating)
        conn.execute(query)
        conn.commit()
        # todo: figure out order of except
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        if DEBUG:
            print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    except Exception as e:
        result = ReturnValue.ERROR
        if DEBUG:
            print(e)
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()

    return result


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


def validateInteger(integer: int):
    if not integer:
        return "Null"

    return integer

# GOOD LUCK!
