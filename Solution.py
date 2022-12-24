from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor
from typing import Union

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
                        Genre TEXT NOT NULL CHECK(Genre = 'Horror' or Genre = 'Comedy' or Genre = 'Action' or Genre = 'Drama')
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

        create_Roles_table = """
                        CREATE TABLE IF NOT EXISTS Roles(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        ActorID INTEGER NOT NULL REFERENCES Actor(ID) ON DELETE CASCADE,
                        Role TEXT NOT NULL,
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year) ON DELETE CASCADE
                        );
                        """

        create_Cast_table = """
                        CREATE TABLE IF NOT EXISTS Casts(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        ActorID INTEGER NOT NULL REFERENCES Actor(ID) ON DELETE CASCADE,
                        Salary INTEGER NOT NULL CHECK (Salary > 0),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year) ON DELETE CASCADE
                        );
                        """

        create_Production_table = """
                        CREATE TABLE IF NOT EXISTS Productions(
                        StudioID INTEGER NOT NULL REFERENCES Studio ON DELETE CASCADE,
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        Budget INTEGER NOT NULL CHECK (Budget >= 0),
                        Revenue INTEGER NOT NULL CHECK (Revenue >= 0),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie ON DELETE CASCADE,
                        UNIQUE (MovieName, MovieYear)
                        );
                        """

        # more...

        # basic tables
        conn.execute(create_critic_table)
        conn.execute(create_movie_table)
        conn.execute(create_actor_table)
        conn.execute(create_studio_table)

        # relations
        conn.execute(create_Ratings_table)
        conn.execute(create_Roles_table)
        conn.execute(create_Cast_table)
        conn.execute(create_Production_table)

        conn.commit()
    except Exception as e:
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def clearTables():
    query = """
            DELETE FROM Critic;
            DELETE FROM Movie;
            DELETE FROM Actor;
            DELETE FROM Studio;
            DELETE FROM Ratings;
            DELETE FROM Casts;
            DELETE FROM Productions;
            """
    execute_query_delete(query)


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "DROP TABLE IF EXISTS Critic CASCADE;"
            "DROP TABLE IF EXISTS Movie CASCADE;"
            "DROP TABLE IF EXISTS Actor CASCADE;"
            "DROP TABLE IF EXISTS Studio CASCADE;"
            "DROP TABLE IF EXISTS Ratings CASCADE;"
            "DROP TABLE IF EXISTS Casts CASCADE;"
            "DROP TABLE IF EXISTS Productions CASCADE;"
        )
        conn.commit()
    except Exception as e:
        if DEBUG:
            print(e)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    critic_id = critic.getCriticID()
    critic_id = validateInteger(critic_id)
    critic_name = critic.getName()
    critic_name = stringQouteMark(critic_name)

    query = "INSERT INTO Critic (ID, Name) VALUES ({critic_id}, {critic_name});"
    query = query.format(critic_id=critic_id, critic_name=critic_name)
    return execute_query_insert(query)


def deleteCritic(critic_id: int) -> ReturnValue:
    query = "DELETE FROM Critic Where ID = {critic_id};"
    query = query.format(critic_id=critic_id)
    return execute_query_delete(query)


def getCriticProfile(critic_id: int) -> Critic:
    critic_id = validateInteger(critic_id)
    result = Critic.badCritic()
    query = "select * FROM Critic Where ID = {critic_id};"
    query = query.format(critic_id=critic_id)
    _, rows_count, rows = execute_query_Select(query)
    if rows_count == 1:
        row = rows[0]
        res_critic_id = row["id"]
        res_critic_name = row["name"]
        result = Critic(res_critic_id, res_critic_name)
    return result


def addActor(actor: Actor) -> ReturnValue:
    actor_id = validateInteger(actor.getActorID())
    actor_name = stringQouteMark(actor.getActorName())
    actor_age = validateInteger(actor.getAge())
    actor_height = validateInteger(actor.getHeight())

    query = "INSERT INTO Actor (ID, Name, Age, Height) VALUES ({actor_id}, {actor_name}, {actor_age}, {actor_height});"
    query = query.format(actor_id=actor_id,
                         actor_name=actor_name,
                         actor_age=actor_age,
                         actor_height=actor_height)
    return execute_query_insert(query)


def deleteActor(actor_id: int) -> ReturnValue:
    query = "DELETE FROM Actor Where (ID = {actor_id});"
    query = query.format(actor_id=actor_id)
    return execute_query_delete(query)


def getActorProfile(actor_id: int) -> Actor:
    result = Actor.badActor()
    query = "select * FROM Actor Where (ID = {actor_id});"
    query = query.format(actor_id=actor_id)
    _, rows_count, rows = execute_query_Select(query)
    if rows_count == 1:
        row = rows[0]
        res_actor_id = row["id"]
        res_actor_name = row["name"]
        res_actor_age = row["age"]
        res_actor_height = row["height"]
        result = Actor(res_actor_id, res_actor_name, res_actor_age, res_actor_height)
    return result


def addMovie(movie: Movie) -> ReturnValue:
    movie_name = stringQouteMark(movie.getMovieName())
    movie_year = validateInteger(movie.getYear())
    movie_genre = stringQouteMark(movie.getGenre())

    query = "INSERT INTO Movie (Name, Year, Genre) VALUES ({movie_name}, {movie_year}, {movie_genre});"
    query = query.format(movie_name=movie_name,
                         movie_year=movie_year,
                         movie_genre=movie_genre)
    return execute_query_insert(query)


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movie_name)
    query = "DELETE FROM Movie Where (Name = {string_movie_name} AND Year = {movie_year});"
    query = query.format(string_movie_name=string_movie_name,
                         movie_year=year)
    return execute_query_delete(query)


def getMovieProfile(movie_name: str, year: int) -> Movie:
    string_movie_name = stringQouteMark(movie_name)
    result = Movie.badMovie()
    query = "select * FROM Movie Where (Name = {string_movie_name} AND Year = {movie_year});"
    query = query.format(string_movie_name=string_movie_name, movie_year=year)
    _, rows_count, rows = execute_query_Select(query)
    if rows_count == 1:
        row = rows[0]
        res_movie_name = row["name"]
        res_movie_year = row["year"]
        res_movie_genre = row["genre"]
        result = Movie(res_movie_name, res_movie_year, res_movie_genre)
    return result


def addStudio(studio: Studio) -> ReturnValue:
    studio_id = validateInteger(studio.getStudioID())
    studio_name = stringQouteMark(studio.getStudioName())

    query = "INSERT INTO Studio (ID, Name) VALUES ({studio_id}, {studio_name});"
    query = query.format(studio_id=studio_id, studio_name=studio_name)
    return execute_query_insert(query)


def deleteStudio(studio_id: int) -> ReturnValue:
    query = "DELETE FROM Studio Where (ID = {studio_id});"
    query = query.format(studio_id=studio_id)
    return execute_query_delete(query)


def getStudioProfile(studio_id: int) -> Studio:
    result = Studio.badStudio()
    query = "SELECT * FROM Studio WHERE (ID = {studio_id});"
    query = query.format(studio_id=studio_id)
    _, rows_count, rows = execute_query_Select(query)
    if rows_count == 1:
        row = rows[0]
        res_studio_id = row["id"]
        res_studio_name = row["name"]
        result = Studio(res_studio_id, res_studio_name)
    return result


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "INSERT INTO Ratings (MovieName, MovieYear, CriticID, rating) VALUES \
                                    ({movieName}, {movieYear}, {criticID}, {rating});"
    query = query.format(movieName=string_movie_name, movieYear=movieYear, criticID=criticID, rating=rating)
    return execute_query_insert(query)


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "DELETE FROM Ratings Where (MovieName = {movieName} AND MovieYear = {movieYear} AND CriticID = {criticID});"
    query = query.format(movieName=string_movie_name, movieYear=movieYear, criticID=criticID)
    return execute_query_delete(query)


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # first need to create roles in the roles table
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "INSERT INTO Productions (studioID, MovieName, MovieYear, budget, revenue) VALUES \
                                    ({studioID}, {movieName}, {movieYear}, {budget}, {revenue});"
    query = query.format(studioID=studioID, movieName=string_movie_name, movieYear=movieYear, budget=budget, revenue=revenue)
    return execute_query_insert(query)


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "DELETE FROM Productions Where (studioID = {studioID} AND movieName = {movieName} AND movieYear = {movieYear});"
    query = query.format(studioID=studioID, movieName=string_movie_name, movieYear=movieYear)
    return execute_query_delete(query)


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    """ returns the average rating of a movie by all critics who rated it. 0 in case of division by zero or movie not found. or other errors
    """
    conn = None
    result = 0.0
    try:
        conn = Connector.DBConnector()
        query = "SELECT AVG(rating) FROM Ratings WHERE MovieName = {movieName} AND MovieYear = {movieYear};"
        query = query.format(movieName=stringQouteMark(movieName), movieYear=movieYear)
        rows_count, rows = conn.execute(query)
        row = rows[0]['avg']
        result = float(row)
        if result is None:
            result = 0.0
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()

    return result


def averageActorRating(actorID: int) -> float:
    """  the average of average ratings of movies in which the actor played,
         or 0 if the actor did not play in any movie.
         if any movie has no ratings, it is counted as having average rating of 0.
         In case the actor does not exist, or have not played in any movies with ratings, return 0. """
    conn = None
    result = 0.0
    try:
        conn = Connector.DBConnector()

        # todo
        # select all movies that the actor played in
        # select all ratings of the movies
        # select the average of the ratings
        # select the average of the averages

        query = query.format(actorID=actorID)
        rows_count, rows = conn.execute(query)
        row = rows[0]['avg']
        result = float(row)
        if result is None:
            result = 0.0
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()

    return result


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


# class DatabaseConnection(Connector.DBConnector):

#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         Connector.DBConnector().close()

# connection = Connector.DBConnector()


# def createConnection() -> Connector.DBConnector:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#     except DatabaseException.ConnectionInvalid as e:
#         if DEBUG:
#             print(e)
#     except Exception as e:
#         if DEBUG:
#             print(e)
#     return conn

def execute_query_insert(query: Union[str, sql.Composed]) -> ReturnValue:
    conn = Connector.DBConnector()
    try:
        conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        if DEBUG:
            print(e)
        return ReturnValue.ERROR
    except Exception as e:
        if DEBUG:
            print(e)
        return ReturnValue.ERROR


def execute_query_delete(query: Union[str, sql.Composed]) -> ReturnValue:
    conn = Connector.DBConnector()
    try:
        rows_count, _ = conn.execute(query)
        if rows_count == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        if DEBUG:
            print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        if DEBUG:
            print(e)
        return ReturnValue.ERROR
    except Exception as e:
        if DEBUG:
            print(e)
        return ReturnValue.ERROR


def execute_query_Select(query: Union[str, sql.Composed]) -> Tuple[ReturnValue, int, Connector.ResultSet]:
    conn = Connector.DBConnector()
    try:
        rows_count, data = conn.execute(query)
        return (ReturnValue.OK, rows_count, data)
    except Exception as e:
        if DEBUG:
            print(e)
        return (ReturnValue.ERROR, 0, 0)
# GOOD LUCK!
