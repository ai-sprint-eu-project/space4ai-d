from Solid.Solid.TabuSearch import TabuSearch
from Solid.Solid.SimulatedAnnealing import SimulatedAnnealing
from Solid.Solid.GeneticAlgorithm import GeneticAlgorithm
from classes.BaseHeuristics import BaseHeuristics
from classes.Logger import Logger
import numpy as np
import copy
from classes.Solution import Configuration, Result
import time

## Tabu Search
#
# Specialization of Algorithm that constructs the optimal solution through a
# Tabu Search approach
class Tabu_Search(BaseHeuristics, TabuSearch):

    ## @var method
    # Method to specify the selected neighbor at each step

    ## Tabu_Search class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param max_time Maximum time of running Tabu search
    #   @param max_steps Maximum iterations of Tabu search
    #   @param initial_state The initial solution obtained by RG as the starting point of TS
    #   @param tabu_size The size of Tabu list
    #   @param max_score Maximum score that the algorithm will stop when it has been reached
    #   @param log Object of Logger.Logger type
    def __init__(self, system, max_time, max_steps, initial_state,
                 tabu_size, max_score=None, log=Logger(), **kwargs):
        BaseHeuristics.__init__(self, system, "TabuSearch", log)
        TabuSearch.__init__(self, initial_state, tabu_size, max_steps, max_time, max_score)
        self.method = "random"

    ## Method to get a list of neigbors
    #   @param self The object pointer
    #   @return A list of solutions (neighbors)
    def _neighborhood(self):
        neighborhood, counter_obj_evaluation = self.union_neighbors(self.current)
        self.counter_obj_evaluation += counter_obj_evaluation
        return [x.solution for x in neighborhood]

    ## Method to get the cost of current solution
    #   @param self The object pointer
    #   @param solution The current solution
    #   @return The cost of current solution
    def _score(self, solution):
        self.counter_obj_evaluation += 1
        return solution.objective_function(self.system)

    ## Method to run TS or LS
    #   @param self The object pointer
    #   @param **kwargs Additional keyword
    #   @return 1) The best solution result
    #           2) The list of costs related to each step
    #           3) The list of best costs obtained so far (up to each current step)
    #           4) The list of time corresponding to each step
    def run_algorithm(self, **kwargs):
        self.logger.level += 1
        self.logger.log("Run Tabu Search", 3)
        best_solution, best_cost, current_cost_list, best_cost_list, time_list = self.run(self.verbose, self.method)
        # initialize results
        result = Result()
        result.solution = best_solution
        self.logger.log("Start check feasibility: {}".format(time.time()), 3)
        feasible = result.check_feasibility(self.system)
        self.logger.log("End check feasibility: {}".format(time.time()), 3)

        if feasible:
            self.logger.log("Solution is feasible", 3)
            # compute cost
            self.logger.log("Compute cost", 3)
            result.objective_function(self.system)
            self.counter_obj_evaluation += 1

        self.logger.level -= 1

        return result, current_cost_list, best_cost_list, time_list


## Local_Search
#
# Specialization of Algorithm that constructs the optimal solution through a
# Local Search approach
class Local_Search(BaseHeuristics, TabuSearch):

    ## @var method
    # Method to specify the selected neighbor at each step

    ## Local_Search class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param max_time Maximum time of running Local search
    #   @param max_steps Maximum iterations of Local search
    #   @param initial_state The initial solution obtained by RG as the starting point of LS
    #   @param max_score Maximum score that the algorithm will stop when it has been reached
    #   @param log Object of Logger.Logger type
    def __init__(self, system, max_time, max_steps, initial_state,
                 min_score=None, log=Logger(), **kwargs):
        BaseHeuristics.__init__(self, system, "LocalSearch", log)
        tabu_size = 1
        TabuSearch.__init__(self, initial_state, tabu_size, max_steps, max_time, min_score)
        self.method = "best"

    ## Method to get a list of neigbors
    #   @param self The object pointer
    #   @return A list of solutions (neighbors)
    def _neighborhood(self):
        neighborhood, counter_obj_evaluation = self.union_neighbors(self.current)
        self.counter_obj_evaluation += counter_obj_evaluation
        return [x.solution for x in neighborhood]

    ## Method to get the cost of current solution
    #   @param self The object pointer
    #   @param solution The current solution
    #   @return The cost of current solution
    def _score(self, solution):
        self.counter_obj_evaluation += 1
        return solution.objective_function(self.system)

    ## Method to run TS or LS
    #   @param self The object pointer
    #   @param **kwargs Additional keyword
    #   @return 1) The best solution result
    #           2) The list of costs related to each step
    #           3) The list of best costs obtained so far (up to each current step)
    #           4) The list of time corresponding to each step
    def run_algorithm(self, **kwargs):

        self.logger.level += 1
        self.logger.log("Run Local Search", 3)
        best_solution, best_cost, current_cost_list, best_cost_list, time_list = self.run(self.verbose, self.method)
        # initialize results
        result = Result()
        result.solution = best_solution
        self.logger.log("Start check feasibility: {}".format(time.time()), 3)
        feasible = result.check_feasibility(self.system)
        self.logger.log("End check feasibility: {}".format(time.time()), 3)

        if feasible:
            self.logger.log("Solution is feasible", 3)
            # compute cost
            self.logger.log("Compute cost", 3)
            result.objective_function(self.system)
            self.counter_obj_evaluation += 1
        else:
            new_result = copy.deepcopy(result)
        self.logger.level -= 1

        return result, current_cost_list, best_cost_list, time_list


## Simulated Annealing

class Simulated_Annealing(BaseHeuristics, SimulatedAnnealing):

    ## Simulated_Annealing class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param max_time Maximum time of running Simulated Annealing
    #   @param max_steps Maximum iterations of Simulated Annealing
    #   @param initial_state The initial solution obtained by RG as the starting point of SA
    #   @param temp_begin The initial temperature
    #   @param schedule_constant The constant value in annealing schedule function
    #   @param schedule 'exponential' or 'linear' annealing schedule
    #   @param min_energy Minimum energy that the algorithm will stop when it has been reached
    #   @param log Object of Logger.Logger type
    def __init__(self, system, max_time, max_steps, initial_state,
                 temp_begin, schedule_constant, schedule, min_energy=None, log=Logger(), **kwargs):
        BaseHeuristics.__init__(self, system, "SimulatedAnnealing", log)
        SimulatedAnnealing.__init__(self, initial_state, temp_begin, schedule_constant,
                                    max_steps, max_time, min_energy, schedule)

    ## Method to get a list of neigbors
    #   @param self The object pointer
    #   @return A solution (neighbor)
    def _neighbor(self):

        neighborhood, counter_obj_evaluation = self.union_neighbors(self.current_state)
        self.counter_obj_evaluation += counter_obj_evaluation
        if len(neighborhood) > 0:
            x = neighborhood[np.argmin([self._energy(x) for x in neighborhood])]
            sol = x.solution
        else:
            sol = None
        return sol

    ## Method to get the cost of current solution
    #   @param self The object pointer
    #   @param solution The current solution
    #   @return The cost of current solution
    def _energy(self, solution):
        self.counter_obj_evaluation += 1
        return solution.objective_function(self.system)

    ## Method to run SA
    #   @param self The object pointer
    #   @param **kwargs Additional keyword
    #   @return 1) The best solution result
    #           2) The list of costs related to each step
    #           3) The list of best costs obtained so far (up to each current step)
    #           4) The list of time corresponding to each step
    def run_algorithm(self, **kwargs):
        self.logger.level += 1
        self.logger.log("Run Simulated Annealing", 3)
        best_solution, best_cost, current_cost_list, best_cost_list, time_list  = self.run(self.verbose)
        # initialize results
        result = Result()
        result.solution = best_solution
        self.logger.log("Start check feasibility: {}".format(time.time()), 3)
        feasible = result.check_feasibility(self.system)
        self.logger.log("End check feasibility: {}".format(time.time()), 3)

        if feasible:
            self.logger.log("Solution is feasible", 3)
            # compute cost
            self.logger.log("Compute cost", 3)
            result.objective_function(self.system)
            self.counter_obj_evaluation += 1
        else:
            new_result = copy.deepcopy(result)
        self.logger.level -= 1

        return result, current_cost_list, best_cost_list, time_list



## Genetic algorithm

class Genetic_Algorithm(BaseHeuristics, GeneticAlgorithm):
    ## @var starting_point
    # Some starting points as initial population

    ## Genetic_Algorithm class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param max_time Maximum time of running Simulated Annealing
    #   @param max_steps Maximum iterations of Simulated Annealing
    #   @param initial_state The initial solutions obtained by RG as the starting points of SA
    #   @param crossover_rate The crossover rate
    #   @param mutation_rate The mutation rate
    #   @param min_fitness Minimum fitness that the algorithm will stop when it has been reached
    #   @param log Object of Logger.Logger type
    def __init__(self, system, max_time, max_steps, initial_state,
                 crossover_rate, mutation_rate, min_fitness=None, log=Logger(), **kwargs):
        BaseHeuristics.__init__(self, system, "Genetic_Algorithm", log)
        GeneticAlgorithm.__init__(self, crossover_rate, mutation_rate,
                                  max_steps, max_time, min_fitness)
        self.starting_point = initial_state

    ## Method to set initial population
    #   @param self The object pointer
    #   @return the starting points as initial population
    def _initial_population(self):
        return self.starting_point

    ## Method to compute fitness
    #   @param self The object pointer
    #   @param member A solution member
    #   @return The fitness of the member
    def _fitness(self, member):

        self.counter_obj_evaluation += 1
        return member.objective_function(self.system)

    ## Method to mutate a member randomly
    #   @param self The object pointer
    #   @param member A solution member
    #   @return A mutated solution member
    def _mutate(self, member):

        if self.mutation_rate >= np.random.random():
            results = None
            fns = [self.change_FaaS, self.change_resource_type, self.change_component_placement,
                   self.move_to_FaaS, self.move_from_FaaS]
            np.random.shuffle(fns)
            while results is None and len(fns) > 0:
                fn = fns.pop()
                results, counter_obj_evaluation = fn(member)
                self.counter_obj_evaluation += counter_obj_evaluation
            if results is not None:
                member = list([result.solution for result in results])
            else:
                member = list([member])
        else:
            member = list([member])
        return member

    ## Method to crossover the parents
    #   @param self The object pointer
    #   @param parent1 First solution as parent1
    #   @param parent2 Second solution as parent2
    #   @return A list of solutions
    def _crossover(self, parent1, parent2):
        # get a partition point randomly
        partition = np.random.randint(0, len(self.population[0].Y_hat) - 1)
        part1 = copy.deepcopy(parent1.Y_hat[0:partition])
        part2 = copy.deepcopy(parent2.Y_hat[partition:])
        children = self.mix_parts(partition, part1, part2)

        part1 = copy.deepcopy(parent2.Y_hat[0:partition])
        part2 = copy.deepcopy(parent1.Y_hat[partition:])
        children.extend(self.mix_parts(partition, part1, part2))
        solutions = []
        for child in children:
            # creat a solution by new child (Y_hat)
            new_solution = Configuration(child)
            # check if new solution is feasible
            performance = new_solution.check_feasibility(self.system)
            if performance[0]:
                solutions.append(new_solution)

        return solutions

    ## Method to mix two parts of two different solutions
    #   @param self The object pointer
    #   @param part1 a part of a solution
    #   @param part2 a part of a solution
    #   @return A list of solutions
    def mix_parts(self, partition, part1, part2):
        act_res1, act_CL1 = self.get_active_res_computationallayers(part1)
        act_res2, act_CL2 = self.get_active_res_computationallayers(part2)

        intersec_CL = set(act_CL1).intersection(act_CL2)
        child1 = copy.deepcopy(part1 + part2)
        children = []
        two_child = False
        if intersec_CL is not None:
            for cl in intersec_CL:
                res_idxs = [compLayer.resources for compLayer in self.system.CLs if compLayer.name == cl][0]
                if res_idxs[0] >= self.system.FaaS_start_index:
                    continue
                res_part1 = set(res_idxs).intersection(act_res1).pop()
                res_part2 = set(res_idxs).intersection(act_res2).pop()
                if res_part1 == res_part2:
                    continue
                else:
                    if not two_child:
                        two_child = True
                        child2 = copy.deepcopy(part1 + part2)
                    # get all partitions of part2 that are running on res_part2
                    partitions2 = self.get_partitions_with_j(part2, res_part2)
                    # get all partitions that are running on res_part1
                    partitions1 = self.get_partitions_with_j(part1, res_part1)
                    # to create child1,
                    for part in partitions2:
                        child1[part[0] + partition][part[1]][res_part2] = 0
                        child1[part[0] + partition][part[1]][res_part1] = part1[partitions1[0][0]][partitions1[0][1]][
                            res_part1]
                    for part in partitions1:
                        child2[part[0]][part[1]][res_part1] = 0

                        child2[part[0]][part[1]][res_part2] = part2[partitions2[0][0]][partitions2[0][1]][res_part2]

        children.append(child1)
        if two_child:
            children.append(child2)

        return children

    ## Method to run GA
    #   @param self The object pointer
    #   @param **kwargs Additional keyword
    #   @return 1) The best solution result
    #           2) The list of costs related to each step
    #           3) The list of time corresponding to each step
    def run_algorithm(self, **kwargs):

        self.logger.level += 1
        self.logger.log("Run Genetic Algorithm", 3)
        best_member, best_fitness, population, best_sol_cost_list, time_list  = self.run(self.verbose)
        # initialize results
        result = Result()
        result.solution = best_member
        self.logger.log("Start check feasibility: {}".format(time.time()), 3)
        feasible = result.check_feasibility(self.system)
        self.logger.log("End check feasibility: {}".format(time.time()), 3)

        if feasible:
            self.logger.log("Solution is feasible", 3)
            # compute cost
            self.logger.log("Compute cost", 3)
            result.objective_function(self.system)
            self.counter_obj_evaluation += 1
        else:
            new_result = copy.deepcopy(result)
        self.logger.level -= 1

        return result, best_sol_cost_list, time_list
