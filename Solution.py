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

        create_Ratings_table = """
                        CREATE TABLE IF NOT EXISTS Ratings(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL CHECK (MovieYear > 1984),
                        CriticID INTEGER NOT NULL,
                        Rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <=5),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year),
                        FOREIGN KEY(CriticID) REFERENCES Critic(ID),
                        UNIQUE(MovieName, MovieYear, CriticID)
                        );
                        """

        create_Roles_table = """
                        CREATE TABLE IF NOT EXISTS Roles(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL CHECK (MovieYear > 1984),
                        ActorID INTEGER NOT NULL,
                        Role TEXT NOT NULL,
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year),
                        FOREIGN KEY(ActorID) REFERENCES Actor(ID),
                        );
                        """
        
        create_Cast_table = """
                        CREATE TABLE IF NOT EXISTS Cast(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL CHECK (MovieYear > 1984),
                        ActorID INTEGER NOT NULL,
                        Salary INTEGER NOT NULL CHECK (Salary > 0),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie(Name, Year),
                        FOREIGN KEY(ActorID) REFERENCES Actor(ID),
                        UNIQUE(MovieName, MovieYear, ActorID)
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
        conn.execute(
                    "DELETE FROM Critic;"
                    "DELETE FROM Movie;"
                    "DELETE FROM Actor;"
                    "DELETE FROM Studio;"
                    "DELETE FROM Ratings;"
                    "DELETE FROM Cast;"
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
        conn.execute(
                     "DROP TABLE IF EXISTS Critic CASCADE;"
                     "DROP TABLE IF EXISTS Movie CASCADE;"
                     "DROP TABLE IF EXISTS Actor CASCADE;"
                     "DROP TABLE IF EXISTS Studio CASCADE;"
                     "DROP TABLE IF EXISTS Ratings CASCADE;"
                     "DROP TABLE IF EXISTS Cast CASCADE;"
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
    conn = None
    
    result = ReturnValue.OK

    if getCriticProfile(critic_id) == Critic.badCritic():
        result = ReturnValue.NOT_EXISTS
        return result

    try:
        conn = Connector.DBConnector()
        query = "DELETE FROM Critic Where ID = {critic_id};"
        query = query.format(critic_id=critic_id)
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    except Exception as e:
        result = ReturnValue.ERROR
        print(e)
        conn.rollback()
    finally:
        if result is not ReturnValue.OK:
            conn.rollback()
        conn.close()
    
    return result


def getCriticProfile(critic_id: int) -> Critic:
    conn = None

    critic_id = critic_id if critic_id is not None else "Null"

    rows_count = 0
    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Critic Where ID = {critic_id};"
        query = query.format(critic_id=critic_id)
        rows_count, rows = conn.execute(query, True)


    except Exception as error:
        print(error)
    finally:
        if rows_count == 0 or rows_count > 1:
            return Critic.badCritic()

        #print("Num of rows: {rows}".format(rows=rows_count))
        row = rows.__getitem__(0)
        res_critic_id = row.__getitem__("id")
        res_critic_name = row.__getitem__("name")
        result = Critic(res_critic_id, res_critic_name)
        if conn is not None:
            conn.close()
        return result


def addActor(actor: Actor) -> ReturnValue:
    conn = None

    actor_id = actor.getActorID()
    actor_id = actor_id if actor_id is not None else "NULL"

    actor_name = actor.getActorName()
    actor_name = stringQouteMark(actor_name)

    actor_age = actor.getAge()
    actor_age = actor_age if actor_age is not None else "NULL"

    actor_height = actor.getHeight()
    actor_height = actor_height if actor_height is not None else "NULL"

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
        if DEBUG:
            print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        if DEBUG:
            print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        if DEBUG:
            print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if DEBUG:
            print(e)
    except Exception as e:
        if DEBUG:
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
    conn = None

    string_movie_name = stringQouteMark(movie_name)
    movie_year = year if year is not None else "Null"
    rows_count = 0
    try:
        conn = Connector.DBConnector()
        rows = Connector.ResultSet()
        query = "select * FROM Movie Where (Name = {string_movie_name} AND Year = {movie_year});"
        query = query.format(string_movie_name=string_movie_name,
                             movie_year=movie_year)
        rows_count, rows = conn.execute(query, True)

    except DatabaseException.NOT_NULL_VIOLATION as e:
        if DEBUG:
            print(e)
    except Exception as error:
        print(error)
    finally:
        if rows_count == 0 or rows_count > 1:
            return Movie.badMovie()

        #print("Num of rows: {rows}".format(rows=rows_count))
        row = rows.__getitem__(0)
        res_movie_name = row["name"]
        res_movie_year = row["year"]
        res_movie_genre = row["genre"]
        result = Movie(res_movie_name, res_movie_year, res_movie_genre)
        if conn is not None:
            conn.close()
        return result


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
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
        if DEBUG:
            print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.ERROR
        print(e)
    except Exception as e:
        result = ReturnValue.ERROR
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
    # first need to create roles in the roles table
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
    """ returns the average rating of a movie by all critics who rated it. 0 in case of division by zero or movie not found. or other errors
    """
    conn = None
    result = 0.0
    try:
        conn = Connector.DBConnector()
        query = "SELECT AVG(rating) FROM Ratings WHERE MovieName = {movieName} AND MovieYear = {movieYear};"
        query = query.format(movieName=stringQouteMark(movieName),
                             movieYear=movieYear)
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

# GOOD LUCK!
