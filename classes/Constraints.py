import sys

from classes.PerformanceEvaluators import SystemPerformanceEvaluator
from abc import ABC, abstractmethod
import numpy as np


## PerformanceConstraint
#
# Class used to represent the performance constraint a configuration is 
# subject to
class PerformanceConstraint(ABC):

    ## @var max_res_time
    # Upper-bound threshold for the response time

    ## PerformanceConstraint class constructor
    #   @param self The object pointer
    #   @param max_res_time Upper-bound threshold for the response time
    def __init__(self, max_res_time):
        self.max_res_time = max_res_time

    ## Method to check the feasibility of the constraint (abstract)
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param solution A Solution.Configuration representing a candidate 
    #                   solution of the problem
    #   @return A boolean which is true if the constraint is satisfied and 
    #           the corresponding response time
    @abstractmethod
    def check_feasibility(self, S, solution):
        pass


## LocalConstraint
#
# Specialization of PerformanceConstraint, related to a single Component
class LocalConstraint(PerformanceConstraint):

    ## @var component_idx
    # Index of the Graph.Component object the constraint is related to 

    ## LocalConstraint class constructor
    #   @param self The object pointer
    #   @param component_idx Index of the Graph.Component object the 
    #                        constraint is related to 
    #   @param max_res_time Upper-bound threshold for the response time
    def __init__(self, component_idx, max_res_time):
        super().__init__(max_res_time)
        self.component_idx = component_idx

    ## Method to check the feasibility of the constraints
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param solution A Solution.Configuration representing a candidate 
    #                   solution of the problem
    #   @return A boolean which is true if the constraint is satisfied and 
    #           the response time of the corresponding Graph.Component
    def check_feasibility(self, S, solution):

        feasible = False

        # evaluate the performance of component
        PE = SystemPerformanceEvaluator(solution.logger)
        perf_evaluation = PE.get_perf_evaluation(S, solution.Y_hat,
                                                 self.component_idx)
        # check if the denumerator is equal to zero
        if not np.isnan(perf_evaluation):
            # update the slack value
            solution.local_slack_value[self.component_idx] = self.max_res_time - perf_evaluation
            # check feasibility
            if perf_evaluation <= self.max_res_time and perf_evaluation > 0:
                feasible = True
        else:
            solution.local_slack_value[self.component_idx] = float('Inf')

        return feasible, perf_evaluation

    ## Operator to convert a LocalConstraint object into a string
    #   @param self The object pointer
    #   @param all_components List of all the Graph.Component objects in the 
    #                         system
    def __str__(self, all_components):
        s = '"{}": {{"local_res_time":{}}}'. \
            format(all_components[self.component_idx].name, self.max_res_time)
        return s


## GlobalConstraint
#
# Specialization of PerformanceConstraint, related to a list of Component 
# objects
class GlobalConstraint(PerformanceConstraint):

    ## @var path
    # List of Component objects the constraint is related to 

    ## @var path_name
    # Name of the path the constraint is related to

    ## GlobalConstraint class constructor
    #   @param self The object pointer
    #   @param path List of indices of Graph.Component objects which are in 
    #               a Path
    #   @param max_res_time Upper-bound threshold for the response time
    #   @param path_name Name of the current path
    def __init__(self, path, max_res_time, path_name=""):
        super().__init__(max_res_time)
        self.path = path
        self.path_name = path_name

    ## Method to check the feasibility of the global constraints
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param solution A Solution.Configuration representing a candidate 
    #                   solution of the problem
    #   @return A boolean which is true if the constraint is satisfied and 
    #           the response time of the Path
    def check_feasibility(self, S, solution):

        feasible = False
        PE = SystemPerformanceEvaluator()

        # compute the response time of all components in the path
        performance_of_components = []
        perf_evaluation = 0
        for comp_index in self.path:
            perf_evaluation = PE.get_perf_evaluation(S, solution.Y_hat,
                                                     comp_index)
            # check if the response time is valid
            if not perf_evaluation == float("inf") \
                    and not np.isnan(perf_evaluation) \
                    and perf_evaluation > 0:
                performance_of_components.append(perf_evaluation)
            else:
                solution.global_slack_value = float('Inf')
                return feasible, float('Inf')

        # sum the response times of all components in the current path
        Sum = sum(performance_of_components)

        # compute network delay
        #
        # loop over all components in the path
        for idx in range(len(self.path) - 1):

            # get the indices of the current and the next component
            comp_index = self.path[idx]
            next_index = self.path[idx + 1]
            if S.components[next_index].name in list(S.graph.G.succ[S.components[comp_index].name].keys()):
                # get the indices of the resources where all partitions of the
                # current and the next component are executed
                j = np.nonzero(solution.Y_hat[comp_index])
                j1 = np.nonzero(solution.Y_hat[next_index])

                # resource index of the last partition
                part1_resource = j[1][-1]
                part2_resource = j1[1][0]

                # check if the two resources are different
                if not part1_resource == part2_resource:
                    # get the names of the two components
                    comp1_key = S.components[comp_index].name
                    comp2_key = S.components[next_index].name
                    # get the amount of transferred data
                    data_size = S.graph.G.get_edge_data(comp1_key, comp2_key)["data_size"]
                    # compute the network delay
                    network_delay = PE.get_network_delay(part1_resource,
                                                         part2_resource,
                                                         S, data_size)
                    # update the total response time of the path
                    Sum += network_delay
            else:
                S.error.log(
                    "Components global constraints path error: component {} is not successor of component {} in the DAG.".format(
                        S.components[next_index].name, S.components[comp_index].name))
                sys.exit(1)

        # compute the slack value of the current solution
        solution.global_slack_value = self.max_res_time - Sum

        # check feasibility
        if Sum <= self.max_res_time:
            feasible = True

        return feasible, Sum

    ## Operator to convert a GlobalConstraint object into a string
    #   @param self The object pointer
    #   @param all_components List of all the Graph.Component objects in the 
    #                         system
    def __str__(self, all_components):
        s = '"' + self.path_name + '": {"components": ['
        for component_idx in self.path:
            s += ('"' + all_components[component_idx].name + '",')
        s = s[:-1] + '], "global_res_time": ' + str(self.max_res_time) + '}'
        return s
