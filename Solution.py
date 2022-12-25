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
        create_Cast_table = """
                        CREATE TABLE IF NOT EXISTS Casts(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        ActorID INTEGER NOT NULL REFERENCES Actor ON DELETE CASCADE,
                        Salary INTEGER NOT NULL CHECK (Salary > 0),
                        FOREIGN KEY(MovieName, MovieYear) REFERENCES Movie ON DELETE CASCADE,
                        UNIQUE(MovieName, MovieYear, ActorID)
                        );
                        """
        create_Roles_table = """
                        CREATE TABLE IF NOT EXISTS Roles(
                        MovieName TEXT NOT NULL,
                        MovieYear INTEGER NOT NULL,
                        ActorID INTEGER NOT NULL,
                        Role TEXT NOT NULL,
                        FOREIGN KEY(MovieName, MovieYear, ActorID) REFERENCES Casts(MovieName, MovieYear, ActorID) ON DELETE CASCADE,
                        UNIQUE(MovieName, MovieYear, ActorID, Role)
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
        create_total_salaries_view = """
                        Create VIEW TotalSalaries AS
                        SELECT name AS MovieName, year AS MovieYear, Coalesce(Sum, 0) as total_salary FROM
		                (SELECT * FROM Movie) AS MovieSum
	                    LEFT OUTER JOIN 
		                (SELECT MovieName, MovieYear, SUM(Salary) FROM CASTS GROUP BY MovieName, MovieYear) AS SalarySum
		                ON MovieSum.Name = SalarySum.MovieName AND MovieSum.Year = SalarySum.MovieYear;
                        """
        create_total_roles_actor_in_movie_view = """
                                                 Create VIEW TotalActorRoles AS
                                                 SELECT moviename, movieYear, actorid, count(roles) AS TOTAL_ACTOR_ROLES FROM roles
                                                 GROUP BY moviename, movieYear, actorid;
                                                 """
        create_actor_casts_view = """
                                  CREATE VIEW ACTORS_CASTS AS
			                      SELECT ID, age As AAge, movieName as CMovieName, movieYear As CMovieYear FROM
                                  (actor INNER JOIN casts
			                      ON actor.id = casts.ActorID );
                                  """

        # more...

        # basic tables
        conn.execute(create_critic_table)
        conn.execute(create_movie_table)
        conn.execute(create_actor_table)
        conn.execute(create_studio_table)

        # relations
        conn.execute(create_Ratings_table)
        conn.execute(create_Cast_table)
        conn.execute(create_Roles_table)
        conn.execute(create_Production_table)

        # views
        conn.execute(create_total_salaries_view)
        conn.execute(create_total_roles_actor_in_movie_view)
        conn.execute(create_actor_casts_view)

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
            DELETE FROM Roles;
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
            "DROP TABLE IF EXISTS Roles CASCADE;"
            "DROP VIEW IF EXISTS TotalSalaries CASCADE;"
            "DROP VIEW IF EXISTS TotalActorRoles CASCADE;"
            "DROP VIEW IF EXISTS ACTORS_CASTS;"
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
    _, rows_count, rows = execute_query_select(query)
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
    _, rows_count, rows = execute_query_select(query)
    if rows_count == 1:
        row = rows[0]
        res_actor_id = row["id"]
        res_actor_name = row["name"]
        res_actor_age = row["age"]
        res_actor_height = row["height"]
        result = Actor(res_actor_id, res_actor_name,
                       res_actor_age, res_actor_height)
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
    _, rows_count, rows = execute_query_select(query)
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
    _, rows_count, rows = execute_query_select(query)
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
    query = query.format(movieName=string_movie_name,
                         movieYear=movieYear, criticID=criticID, rating=rating)
    return execute_query_insert(query)


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "DELETE FROM Ratings Where (MovieName = {movieName} AND MovieYear = {movieYear} AND CriticID = {criticID});"
    query = query.format(movieName=string_movie_name,
                         movieYear=movieYear, criticID=criticID)
    return execute_query_delete(query)


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)

    query_roles = ""
    for role in roles:
        string_role = stringQouteMark(role)
        query_to_add = "({movieName}, {movieYear}, {actorId}, {string_role}),"
        query_to_add = query_to_add.format(movieName=string_movie_name,
                                           movieYear=movieYear,
                                           actorId=actorID,
                                           string_role=string_role)
        query_roles += query_to_add
    query_roles = query_roles[:-1] + \
        ";" if not query_roles == "" else "(Null, Null, Null, Null);"
    query = """
            INSERT INTO Casts (MovieName, MovieYear, actorId, salary) VALUES
            ({movieName}, {movieYear}, {actorId}, {salary});
            INSERT INTO Roles (MovieName, MovieYear, actorId, Role) VALUES 
            """
    query = query + '\n' + query_roles
    query = query.format(movieName=string_movie_name,
                         movieYear=movieYear, actorId=actorID, salary=salary)
    return execute_query_insert(query)


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "DELETE FROM Casts Where (movieName = {movieName} AND movieYear = {movieYear} AND actorID = {actorID});"
    query = query.format(movieName=string_movie_name,
                         movieYear=movieYear, actorID=actorID)
    return execute_query_delete(query)


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "INSERT INTO Productions (studioID, MovieName, MovieYear, budget, revenue) VALUES \
                                    ({studioID}, {movieName}, {movieYear}, {budget}, {revenue});"
    query = query.format(studioID=studioID, movieName=string_movie_name,
                         movieYear=movieYear, budget=budget, revenue=revenue)
    return execute_query_insert(query)


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    string_movie_name = stringQouteMark(movieName)
    query = "DELETE FROM Productions Where (studioID = {studioID} AND movieName = {movieName} AND movieYear = {movieYear});"
    query = query.format(
        studioID=studioID, movieName=string_movie_name, movieYear=movieYear)
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
        query = query.format(movieName=stringQouteMark(
            movieName), movieYear=movieYear)
        rows_count, rows = conn.execute(query)
        row = rows[0]['avg']
        result = float(row) if row else None
        if result is None:
            result = 0.0
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()

    return result


"""  the average of average ratings of movies in which the actor played,
         or 0 if the actor did not play in any movie.
         if any movie has no ratings, it is counted as having average rating of 0.
         In case the actor does not exist, or have not played in any movies with ratings, return 0. """


def averageActorRating(actorID: int) -> float:
    result = 0.0
    query = """
            SELECT Avg(single_movie_rating) FROM (
            SELECT Casts.MovieName, Casts.MovieYear, COALESCE(AVG(rating), 0) AS single_movie_rating
            FROM Casts
            LEFT OUTER JOIN Ratings
            ON Casts.MovieName = Ratings.MovieName AND Casts.MovieYear = Ratings.MovieYear
            WHERE actorID = {actorID}
            GROUP BY Casts.MovieName, Casts.MovieYear
            ) AS TEMPORARAY_NAME
            """
    query = query.format(actorID=actorID)
    ret_res, rows_count, rows = execute_query_select(query)
    if rows_count == 1 and ret_res == ReturnValue.OK:
        result = float(rows[0]['avg']) if rows[0]['avg'] else result
    return result


"""
Input: ID of the actor
Output: Movie object of the best rated (by average rating) movie the actor has played in.
If multiple movies share the highest average rating, tie breaker is done by choosing the
earlier release, and for movies released in the same year choosing the movie with greater
name (lexicographically)
In case the actor doesn’t exist or did not play in any movies, return badMovie()
"""


def bestPerformance(actor_id: int) -> Movie:

    result = Movie.badMovie()
    query = """
            SELECT movie.name, movie.year, movie.genre, Castings.rating FROM
	        movie INNER JOIN (
			SELECT Casts.MovieName, Casts.MovieYear, COALESCE(rating, -1) AS rating
	        FROM Casts LEFT OUTER JOIN Ratings
            ON Casts.MovieName = Ratings.MovieName AND Casts.MovieYear = Ratings.MovieYear
            WHERE actorID = {actor_id}
		    ) AS Castings
		    ON Castings.MovieName = movie.name AND Castings.MovieYear = movie.Year
	        ORDER BY 
			Castings.rating DESC,
	        Castings.MovieYear ASC,
	        Castings.MovieNAME DESC
			LIMIT 1
            """
    query = query.format(actor_id=actor_id)
    ret_res, rows_count, rows = execute_query_select(query)
    if rows_count == 1 and ret_res == ReturnValue.OK:
        row = rows[0]
        res_movie_name = row["name"]
        res_movie_year = row["year"]
        res_movie_genre = row["genre"]
        result = Movie(res_movie_name, res_movie_year, res_movie_genre)
    return result


"""
Input: name and year of the movie
Output: the difference between the budget of the movie and the sum of salaries of actors
who play in the movie. Movies that was not produced by any studio, are considered to have a
budget of 0.
In case the movie does not exist, return -1
"""


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    string_movie_name = stringQouteMark(movieName)
    totalCrewBudget = -1
    query = """
        SELECT COALESCE(budget, 0)-total_salary AS diff FROM 
		(SELECT * FROM TotalSalaries  WHERE MovieName = {string_movie_name} AND MovieYear = {movieYear}) AS Movie_salary
		LEFT OUTER JOIN
		Productions P
		ON Movie_salary.MovieName = P.MovieName AND Movie_salary.MovieYear = P.MovieYear
    """
    query = query.format(string_movie_name=string_movie_name,
                         movieYear=movieYear)
    _, rows_count, rows = execute_query_select(query)
    if rows_count == 1:
        totalCrewBudget = rows[0]["diff"]
    return totalCrewBudget


"""
Input:  The name and year of the movie and the ID of the actor.
Output: Returns True if the actor with actor_id plays at least half of all the roles in the movie.
        False otherwise
        Returns False if either the movie or actor do not exist, or if the actor does not play
        any role in the movie
"""


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    string_movie_name = stringQouteMark(movie_name)
    invested = False
    query = """
            SELECT (cast(total_actor_roles as decimal)/cast(total_roles as decimal)) >= 0.5 AS invested FROM (
            SELECT * FROM TotalActorRoles WHERE movieName={string_movie_name} AND movieYear={movie_year} AND actorID={actor_id}
            ) AS TOTAL_ACTOR_ROLES_SELECT
            INNER JOIN
            (SELECT moviename, movieYear, count(roles) as TOTAL_ROLES FROM roles
            GROUP BY moviename, movieYear) AS TOTAL_MOVIE_ROLES
            ON TOTAL_ACTOR_ROLES_SELECT.movieName = TOTAL_MOVIE_ROLES.movieName AND TOTAL_ACTOR_ROLES_SELECT.movieYear = TOTAL_MOVIE_ROLES.movieYear
            """
    query = query.format(string_movie_name=string_movie_name,
                         movie_year=movie_year,
                         actor_id=actor_id)
    _, rows_count, rows = execute_query_select(query)
    if rows_count == 1:
        invested = rows[0]["invested"]
    return invested


# ---------------------------------- ADVANCED API: ----------------------------------
"""
Input: None
Output: list of (movie_name, total_revenue). Where total_revenue is the sum of all revenues
movies with movie_name made for studios. If a movie was not produced by any studio, its
revenue should be counted as 0.
the movies should be ordered by name in descending order.
"""


def franchiseRevenue() -> List[Tuple[str, int]]:
    franchiseList = []
    query = """
        SELECT movie.name, SUM(COALESCE(revenue, 0)) as TOTAL_REVENUE FROM
	    movie LEFT OUTER JOIN productions
	    on movie.name = productions.moviename and movie.year = productions.movieyear
	    GROUP BY movie.name
	    ORDER BY
	    movie.name DESC
        """
    _, _, rows = execute_query_select(query)
    return rows.rows


"""
Input: None
Output:
list of (studio_id, year, total_revenue). Where total_revenue is the sum of all revenues
movies with made for the studio with studio_id during that year.
The tuples should be ordered by studio_id in descending order, and tuples with the same
studios should be ordered by year in descending order.
"""


def studioRevenueByYear() -> List[Tuple[str, int]]:
    query = """
        SELECT studioid, movieyear, SUM(revenue) as total_revenue_year FROM PRODUCTIONS
        GROUP BY studioid, movieyear
        ORDER BY
        studioid DESC,
        movieyear DESC
        """
    _, _, rows = execute_query_select(query)
    return rows.rows


"""
We will define a critic to be a fan of a studio, if he rated every movie produced by the studio.

Input: None
Output: list of (critic_id, studio_id) where the critic with critic_id is a fan of studio with
studio_id
The list should be ordered by CriticID in descending order, and tuples with the same critic
should be ordered by StudioID in descending order.
"""


def getFanCritics() -> List[Tuple[int, int]]:

    query = """
            SELECT RATINGS_FOR_STUDIO.criticid, MOVIES_PER_STUDIO.studioid FROM(
            SELECT criticid, count(studioid) AS Rated, studioid FROM ratings R RIGHT OUTER JOIN Productions P
	        ON R.moviename = P.movieName AND R.movieYear = P.movieYear
	        GROUP BY criticid, studioid) AS RATINGS_FOR_STUDIO
	        RIGHT OUTER JOIN (
	        SELECT count(studioid) AS Produced, studioid FROM PRODUCTIONS
	        GROUP BY studioid
	        ) AS MOVIES_PER_STUDIO
	        on RATINGS_FOR_STUDIO.studioid = MOVIES_PER_STUDIO.studioid AND RATINGS_FOR_STUDIO.Rated = MOVIES_PER_STUDIO.Produced
	        WHERE RATINGS_FOR_STUDIO.criticid IS NOT NULL
			ORDER BY
			RATINGS_FOR_STUDIO.criticid DESC,
			MOVIES_PER_STUDIO.studioid DESC;
            """
    _, _, rows = execute_query_select(query)
    return rows.rows


"""
Input: None
Output: list of (genre, average_age) where average_age is the average age of actors who play
in at least one movie of the genre.
NOTE: an actor might play in multiple movies from the same genre, they still should only be
counted once.
"""


def averageAgeByGenre() -> List[Tuple[str, float]]:
    query = """
            SELECT genre, avg(aage) FROM
            ACTORS_CASTS INNER JOIN movie
            ON ACTORS_CASTS.CmovieName = movie.Name AND ACTORS_CASTS.CmovieYear = movie.year
            GROUP BY genre
            ORDER BY genre ASC;
            """
    _, _, rows = execute_query_select(query)
    return rows.rows

"""
Input: None
Output: a list of (actor_id, studio_id) where the actor with actor_id played only in movies
Produced by studio with Studio_id.
The list should be ordered by actor_id in descending order.
"""
def getExclusiveActors() -> List[Tuple[int, int]]:
    query = """
            SELECT id, studioid
            FROM ACTORS_CASTS INNER JOIN Productions
            on cmoviename = productions.moviename and cmovieyear = productions.movieyear
            GROUP BY id, studioid
            HAVING COUNT(DISTINCT studioID) = 1
            ORDER BY
            id DESC;
            """
    _, _, rows = execute_query_select(query)
    return rows.rows


def stringQouteMark(string: str) -> str:
    if not string:
        return "Null"

    return "'" + string + "'"


def validateInteger(integer: int):
    if not integer:
        return "Null"

    return integer


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


def execute_query_select(query: Union[str, sql.Composed]) -> Tuple[ReturnValue, int, Connector.ResultSet]:
    conn = Connector.DBConnector()
    try:
        rows_count, data = conn.execute(query)
        return (ReturnValue.OK, rows_count, data)
    except Exception as e:
        if DEBUG:
            print(e)
        return (ReturnValue.ERROR, 0, 0)
# GOOD LUCK!
