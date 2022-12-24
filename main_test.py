import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest

from Business.Critic import Critic
from Business.Actor import Actor
from Business.Movie import Movie
from Business.Studio import Studio

from random import randint

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):

    def testAddRemoveCriticRatedMovie(self) -> None:
        jhon = Critic(
            critic_id=1, critic_name="John")
        mission_impossible = Movie(
            movie_name="Mission Impossible", year="1996", genre="Action")
        self.assertEqual(ReturnValue.OK, Solution.addMovie(
            mission_impossible), "Added New Movie - OK")
        self.assertEqual(ReturnValue.OK, Solution.addCritic(
            jhon), "Added New Critic - OK")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addCritic(
            jhon), "Added New Critic - AlreadyExists")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addMovie(
            mission_impossible), "Added New Movie - AlreadyExists")
        self.assertEqual(ReturnValue.OK, Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), jhon.getCriticID(), 3), "CriticRatedMovie - jhon, MI, 3 - OK")
        self.assertEqual(
            ReturnValue.OK, Solution.deleteCritic(jhon.getCriticID()), "Removed jhon - OK")
        self.assertEqual(ReturnValue.OK, Solution.addCritic(
            jhon), "Added New Critic - OK")
        self.assertEqual(ReturnValue.OK, Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), jhon.getCriticID(), 3), "CriticRatedMovie - jhon, MI, 3 - OK")
        self.assertEqual(
            ReturnValue.OK, Solution.deleteMovie(mission_impossible.getMovieName(), mission_impossible.getYear()), "Removed MI - OK")

    def testCritic(self) -> None:
        invalid_critic = Critic(critic_id=1, critic_name=None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addCritic(
            invalid_critic), "invalid name")
        invalid_critic = Critic(critic_id=None, critic_name="John")
        self.assertEqual(ReturnValue.BAD_PARAMS,
                         Solution.addCritic(invalid_critic), "invalid id")
        jhon = Critic(critic_id=1, critic_name="John")
        self.assertEqual(
            ReturnValue.OK, Solution.addCritic(jhon), "valid 1critic")
        bob = Critic(critic_id=1, critic_name="Bob")
        self.assertEqual(ReturnValue.ALREADY_EXISTS,
                         Solution.addCritic(bob), "existing id")

    def testActorPlayedInMovie(self) -> None:
        leonardo = Actor(
            actor_id=1, actor_name="Leonardo DiCaprio", age=48, height=183)
        self.assertEqual(ReturnValue.OK, Solution.addActor(
            leonardo), "Added LEO DI - OK")
        mission_impossible = Movie(
            movie_name="Mission Impossible", year="1996", genre="Action")
        self.assertEqual(ReturnValue.OK, Solution.addMovie(
            mission_impossible), "should work")
        roles = ["actor1", "actor2"]
        self.assertEqual(ReturnValue.OK, Solution.actorPlayedInMovie(mission_impossible.getMovieName(), mission_impossible.getYear(), leonardo.getActorID(), 1000, roles), "should work")

    def testActor(self) -> None:
        invalid_actor = Actor(
            actor_id=None, actor_name=None, age=-500, height=0)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addActor(
            invalid_actor), "invalid parameters")
        leonardo = Actor(
            actor_id=1, actor_name="Leonardo DiCaprio", age=48, height=183)
        self.assertEqual(ReturnValue.OK, Solution.addActor(
            leonardo), "should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS,
                         Solution.addActor(leonardo), "already exists")

    def testMovie(self) -> None:
        invalid_movie = Movie(
            movie_name="Mission Impossible", year="343", genre="Action")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addMovie(
            invalid_movie), "invalid year")
        # note that postgreSQL will convert this string to an int
        mission_impossible = Movie(
            movie_name="Mission Impossible", year="1996", genre="Action")
        self.assertEqual(ReturnValue.OK, Solution.addMovie(
            mission_impossible), "should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addMovie(
            mission_impossible), "already exists")

    def testStudio(self) -> None:
        invalid_studio = Studio(studio_id=None, studio_name="Warner Bros")
        self.assertEqual(ReturnValue.BAD_PARAMS,
                         Solution.addStudio(invalid_studio), "invalid id")
        warner_bros = Studio(studio_id=1, studio_name="Warner Bros")
        self.assertEqual(ReturnValue.OK, Solution.addStudio(
            warner_bros), "should work")
        paramount = Studio(studio_id=1, studio_name="Paramount")
        self.assertEqual(ReturnValue.ALREADY_EXISTS,
                         Solution.addStudio(paramount), "ID 1 already exists")

    def testAddCriticRemoveCritic(self) -> None:
        jhon = Critic(critic_id=1, critic_name="John")
        self.assertEqual(
            ReturnValue.OK, Solution.addCritic(jhon), "valid 2critic")
        self.assertEqual(
            jhon, Solution.getCriticProfile(jhon.getCriticID()), "Same Critic")
        self.assertEqual(
            ReturnValue.OK, Solution.deleteCritic(1), "Removed")
        self.assertEqual(ReturnValue.NOT_EXISTS,
                         Solution.deleteCritic(1), "No Such Critic")

    def testCriticRatedMovie(self) -> None:
        jhon = Critic(critic_id=1, critic_name="John")
        self.assertEqual(
            ReturnValue.OK, Solution.addCritic(jhon), "valid critic")
        mission_impossible = Movie(
            movie_name="Mission Impossible", year="1996", genre="Action")
        self.assertEqual(
            ReturnValue.OK, Solution.addMovie(mission_impossible), "should work")
        self.assertEqual(
            ReturnValue.OK, Solution.criticRatedMovie(mission_impossible.getMovieName(), mission_impossible.getYear(), jhon.getCriticID(), 3), "Added Critic OK")
        self.assertEqual(
            ReturnValue.ALREADY_EXISTS, Solution.criticRatedMovie(mission_impossible.getMovieName(), mission_impossible.getYear(), jhon.getCriticID(), 3), "already exists")
        self.assertEqual(
            ReturnValue.BAD_PARAMS, Solution.criticRatedMovie(mission_impossible.getMovieName(), mission_impossible.getYear(), jhon.getCriticID(), 9), "rating > 5")
        self.assertEqual(
            ReturnValue.NOT_EXISTS, Solution.criticRatedMovie("mission", mission_impossible.getYear(), jhon.getCriticID(), 3), "moviename = none")
        self.assertEqual(
            ReturnValue.NOT_EXISTS, Solution.criticRatedMovie(mission_impossible.getMovieName(), "1991", jhon.getCriticID(), 3), "movieyear = none")
        self.assertEqual(
            ReturnValue.NOT_EXISTS, Solution.criticRatedMovie(mission_impossible.getMovieName(), mission_impossible.getYear(), 123, 3), "criticid = none")

    def testAverageMovie(self):
        john = Critic(critic_id=1, critic_name="John")
        markus = Critic(critic_id=2, critic_name="Markus")
        amit = Critic(critic_id=3, critic_name="Amit")
        ranjit = Critic(critic_id=4, critic_name="Ranjit")
        petrus = Critic(critic_id=5, critic_name="Petrus")
        claudius = Critic(critic_id=6, critic_name="Claudius")
        Solution.addCritic(john)
        Solution.addCritic(markus)
        Solution.addCritic(amit)
        Solution.addCritic(ranjit)
        Solution.addCritic(petrus)
        Solution.addCritic(claudius)
        mission_impossible = Movie(
            movie_name="Mission Impossible", year="1996", genre="Action")
        Solution.addMovie(mission_impossible)

        self.assertEqual(0.0, Solution.averageRating(
            mission_impossible.getMovieName(), mission_impossible.getYear()), "average rating")

        ratings = [randint(1, 5) for i in range(6)]
        avg = sum(ratings) / len(ratings)

        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), john.getCriticID(), ratings[0])
        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), markus.getCriticID(), ratings[1])
        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), amit.getCriticID(), ratings[2])
        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), ranjit.getCriticID(), ratings[3])
        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), petrus.getCriticID(), ratings[4])
        Solution.criticRatedMovie(mission_impossible.getMovieName(
        ), mission_impossible.getYear(), claudius.getCriticID(), ratings[5])

        self.assertEqual(avg, Solution.averageRating(
            mission_impossible.getMovieName(), mission_impossible.getYear()), "average rating")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    Solution.dropTables()
    Solution.createTables()
    Solution.clearTables()
    unittest.main(verbosity=2, exit=False)
    Solution.dropTables()
