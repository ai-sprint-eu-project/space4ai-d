from abc import ABC, abstractmethod
from classes.Logger import Logger
from classes.Solution import Configuration, Result, EliteResults
import numpy as np
import copy
import sys
import time


## BaseAlgorithm
# Abstract class to represent the algorithm used for searching solution space
class BaseAlgorithm(ABC):
    ## @var keyword
    # Keyword identifying the algorithm

    ## Algorithm class constructor:
    #   @param self The object pointer
    #   @param keyword Keyword identifying the algorithm
    #   @param **kwargs Additional keyword
    def __init__(self, keyword, **kwargs):
        self.keyword = keyword

    ## Method to run the corresponding algorithm
    #   @param self The object pointer
    #   @param **parameters The parameters of algorithm
    #   @return Solution results
    @abstractmethod
    def run_algorithm(self, **parameters):
        pass

    ## Operator to convert an algorithm object into a string
    #   @param self The object pointer
    def __str__(self):
        s = '"algorithm":"{}"'. \
            format(self.keyword)
        return s


## RandomGreedy
#
# Specialization of Algorithm that constructs the optimal solution through a
# randomized greedy approach
class RandomGreedy(BaseAlgorithm):

    ## RandomGreedy class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param seed A seed to generate a repeatable random sequence
    #   @param max_time Maximum time needed to run the algorithm
    #   @param max_steps Maximum iterations needed to run the algorithm
    #   @param k_best The number of top best solutions that the algorithm must returns
    #   @param log Object of Logger.Logger type
    def __init__(self, system, seed, max_time=1, max_steps=1, k_best=1, log=Logger()):
        super().__init__("RandomGreedy")
        self.system = system
        self.seed = seed
        self.max_time = max_time
        self.max_iterations = max_steps
        self.k_best = k_best
        self.logger = log
        self.error = Logger(stream=sys.stderr, verbose=1, error=True)
        np.random.seed(seed)


    ## Method to create the initial random solution
    #   @param self The object pointer
    #   @return (1) List of 2D numpy matrices denoting the amount of
    #           Resources.Resource assigned to each
    #           Graph.Component.Partition object
    #           (2) List of lists that store the random numbers used to select
    #           the resource assigned to each component partition in the chosen
    #           deployment
    #           (3) List of randomly selected numbers of each used edge/cloud
    #           resource
    #           (4) List of indices of the resource randomly selected in each
    #           computational layer
    def create_random_initial_solution(self):

        # increase indentation level for logging
        self.logger.level += 1

        # initialize the assignments
        self.logger.log("Initialize matrices", 4)
        CL_res_random = []
        res_parts_random = []
        y_hat = []
        y = []

        # loop over all components
        I = len(self.system.components)
        for i in range(I):
            # get the number of partitions and available resources
            H, J = self.system.compatibility_matrix[i].shape
            # create the empty matrices
            y_hat.append(np.full((H, J), 0, dtype=int))
            y.append(np.full((H, J), 0, dtype=int))

        # generate the list of candidate nodes, selecting one node per each
        # computational layer (and all nodes in the FaaS layers)
        self.logger.log("Generate candidate resources", 4)
        candidate_nodes = []
        resource_count = 0
        # loop over all computational layers
        for l in self.system.CLs:
            # select all nodes in FaaS layers
            if resource_count >= self.system.FaaS_start_index:
                random_num = l.resources
                candidate_nodes.extend(random_num)
            # randomly select a node in other layers
            else:
                random_num = np.random.choice(l.resources)
                CL_res_random.append(l.resources.index(random_num))
                candidate_nodes.append(random_num)
            resource_count += len(l.resources)

        # loop over all components
        self.logger.log("Assign components", 4)
        for comp in self.system.components:

            # randomly select a deployment for that component
            random_dep = np.random.choice(comp.deployments)
            h = 0
            rand = []
            # loop over all partitions in the deployment
            for part_idx in random_dep.partitions_indices:
                part = comp.partitions[part_idx]
                # get the indices of the component and the deployment
                i = self.system.dic_map_part_idx[comp.name][part.name][0]
                h_idx = self.system.dic_map_part_idx[comp.name][part.name][1]
                # get the indices of compatible resources and compute the
                # intersection with the selected resources in each
                # computational layer
                idx = np.nonzero(self.system.compatibility_matrix[i][h_idx, :])[0]
                index = list(set(candidate_nodes).intersection(idx))
                # randomly extract a resource index in the intersection
                if len(index) < 1:
                    y_hat, res_parts_random, VM_numbers, CL_res_random = [None, None, None, None]
                    return y_hat, res_parts_random, VM_numbers, CL_res_random
                prob = 1 / len(index)
                step = 0
                rn = np.random.random()
                rand.append(rn)
                for r in np.arange(0, 1, prob):
                    if rn > r and rn <= r + prob:
                        j = index[step]
                    else:
                        step += 1
                y[i][h_idx, j] = 1
                y_hat[i][h_idx, j] = 1
                # if the partition is the last partition (i.e., its successor
                # is the successor of the component), update the size of
                # data transferred between the components
                if self.system.graph.G.succ[comp.name] != {}:
                    # if part.Next == list(self.system.graph.G.succ[comp.name].keys())[0]:
                    if part.Next == list(self.system.graph.G.succ[comp.name].keys()):
                        for next_idx in range(len(part.Next)):
                            self.system.graph.G[comp.name][part.Next[next_idx]]["data_size"] = part.data_size[
                                next_idx]

            res_parts_random.append(rand)

        # loop over edge/cloud resources
        self.logger.log("Set number of resources", 4)
        VM_numbers = []
        for j in range(self.system.FaaS_start_index):
            # randomly generate the number of resources that can be assigned
            # to the partitions that run on that resource
            number = np.random.randint(1, self.system.resources[j].number + 1)
            VM_numbers.append(number - 1)
            # loop over components
            for i in range(I):
                # get the number of partitions
                H = self.system.compatibility_matrix[i].shape[0]
                # loop over the partitions
                for h in range(H):
                    # if the partition runs on the current resource, update
                    # the number
                    if y[i][h][j] > 0:
                        y_hat[i][h][j] = y[i][h][j] * number

        self.logger.level -= 1

        return y_hat, res_parts_random, VM_numbers, CL_res_random

    ## Method to create the initial random solution smartly
    #   @param self The object pointer
    #   @return (1) List of 2D numpy matrices denoting the amount of
    #           Resources.Resource assigned to each
    #           Graph.Component.Partition object
    #           (2) List of lists that store the random numbers used to select
    #           the resource assigned to each component partition in the chosen
    #           deployment
    #           (3) List of randomly selected numbers of each used edge/cloud
    #           resource
    #           (4) List of indices of the resource randomly selected in each
    #           computational layer
    def create_random_initial_solution_smart(self):

        # increase indentation level for logging
        self.logger.level += 1

        # initialize the assignments
        self.logger.log("Initialize matrices", 4)
        CL_res_random = []
        res_parts_random = []
        y_hat = []
        y = []

        # loop over all components
        I = len(self.system.components)
        for i in range(I):
            # get the number of partitions and available resources
            H, J = self.system.compatibility_matrix[i].shape
            # create the empty matrices
            y_hat.append(np.full((H, J), 0, dtype=int))
            y.append(np.full((H, J), 0, dtype=int))

        # generate the list of candidate nodes, selecting one node per each
        # computational layer (and all nodes in the FaaS layers)
        self.logger.log("Generate candidate resources", 4)
        candidate_nodes = []
        resource_count = 0
        # loop over all computational layers
        for l in self.system.CLs:
            # select all nodes in FaaS layers
            if resource_count >= self.system.FaaS_start_index:
                random_num = l.resources
                candidate_nodes.extend(random_num)
            # randomly select a node in other layers
            else:
                random_num = np.random.choice(l.resources)
                CL_res_random.append(l.resources.index(random_num))
                candidate_nodes.append(random_num)
            resource_count += len(l.resources)

        # loop over all components
        self.logger.log("Assign components", 4)
        source_nodes = [node[0] for node in self.system.graph.G.in_degree if node[1] == 0]
        visited = {node: False for node in self.system.graph.G.nodes}
        Queue = copy.deepcopy(source_nodes)
        possible_res = copy.deepcopy(self.system.compatibility_matrix)
        # for node in Queue:
        #   visited[node]=True
        while Queue:
            pred_is_visited = True
            current_node = Queue.pop(0)
            comp_idx = self.system.dic_map_com_idx[current_node]
            comp = self.system.components[comp_idx]
            random_dep = np.random.choice(comp.deployments)
            comp_pred_ist = list(self.system.graph.G.pred[current_node])
            if len(comp_pred_ist) > 0:
                for comp_pred in comp_pred_ist:
                    comp_pred_idx = self.system.dic_map_com_idx[comp_pred]
                    if len(np.nonzero(y_hat[comp_pred_idx])[0]) > 0:
                        last_h_idx = np.nonzero(y_hat[comp_pred_idx])[0][-1]
                        last_h_res = np.nonzero(y_hat[comp_pred_idx][last_h_idx, :])[0][0]
                        if last_h_res >= self.system.cloud_start_index:
                            possible_res[comp_idx][:, :self.system.cloud_start_index] = 0
                    else:
                        pred_is_visited = False
                        if comp_pred in Queue:
                            Queue.remove(comp_pred)
                        Queue.insert(0, comp_pred)
                        if current_node in Queue:
                            Queue.remove(current_node)
                        Queue.insert(1, current_node)
            if pred_is_visited:
                last_part_res = -1
                h = 0
                rand = []
                # loop over all partitions in the deployment
                for part_idx in random_dep.partitions_indices:
                    part = comp.partitions[part_idx]
                    # get the indices of the component and the deployment
                    i = self.system.dic_map_part_idx[comp.name][part.name][0]
                    h_idx = self.system.dic_map_part_idx[comp.name][part.name][1]
                    if last_part_res >= self.system.cloud_start_index:
                        possible_res[comp_idx][:, :self.system.cloud_start_index] = 0
                    idx = np.nonzero(possible_res[i][h_idx, :])[0]
                    index = list(set(candidate_nodes).intersection(idx))
                    prob = 1 / len(index)
                    step = 0
                    rn = np.random.random()
                    rand.append(rn)
                    for r in np.arange(0, 1, prob):
                        if rn > r and rn <= r + prob:
                            j = index[step]
                        else:
                            step += 1
                    last_part_res = j
                    y[i][h_idx, j] = 1
                    y_hat[i][h_idx, j] = 1
                    if self.system.graph.G.succ[comp.name] != {}:
                        # if part.Next == list(self.system.graph.G.succ[comp.name].keys())[0]:
                        if part.Next == list(self.system.graph.G.succ[comp.name].keys()):
                            for next_idx in range(len(part.Next)):
                                self.system.graph.G[comp.name][part.Next[next_idx]]["data_size"] = part.data_size[
                                    next_idx]
                visited[current_node] = True
                for node in self.system.graph.G.neighbors(current_node):
                    if not visited[node]:
                        if node not in Queue:
                            Queue.append(node)

        # loop over edge/cloud resources
        self.logger.log("Set number of resources", 4)
        VM_numbers = []
        for j in range(self.system.FaaS_start_index):
            # randomly generate the number of resources that can be assigned
            # to the partitions that run on that resource
            number = np.random.randint(1, self.system.resources[j].number + 1)
            VM_numbers.append(number - 1)
            # loop over components
            for i in range(I):
                # get the number of partitions
                H = self.system.compatibility_matrix[i].shape[0]
                # loop over the partitions
                for h in range(H):
                    # if the partition runs on the current resource, update
                    # the number
                    if y[i][h][j] > 0:
                        y_hat[i][h][j] = y[i][h][j] * number

        self.logger.level -= 1

        return y_hat, res_parts_random, VM_numbers, CL_res_random

    ## Single step of the randomized greedy algorithm: it randomly generates
    # a candidate solution, then evaluates its feasibility. If it is feasible,
    # it evaluates its cost and updates it by reducing the cluster size
    #   @param self The object pointer
    #   @return Two tuples, storing the solution, its cost and its
    #           performance results before and after the update, and a tuple
    #           storing all the random parameters
    def step(self):

        # increase indentation level for logging
        self.logger.level += 1
        self.logger.log("Randomized Greedy step", 3)

        # initialize results
        result = Result(self.logger)

        # generate random solution and check its feasibility
        self.logger.level += 1
        self.logger.log("Generate random solution", 3)
        y_hat, res_parts_random, VM_numbers_random, CL_res_random = self.create_random_initial_solution()
        if y_hat == None:
            self.logger.log("The random solution is None")
            new_result = copy.deepcopy(result)
        else:
            self.logger.log("Random solution is generated")
            result.solution = Configuration(y_hat, self.logger)
            self.logger.log("Start check feasibility: {}".format(time.time()))
            performance = result.check_feasibility(self.system)
            self.logger.log("End check feasibility: {}".format(time.time()))

            # if the solution is feasible, compute the corresponding cost
            # before and after updating the clusters size
            if performance[0]:
                self.logger.log("Solution is feasible")
                # compute cost
                self.logger.log("Compute cost", 3)
                result.objective_function(self.system)
                # update the cluster size of cloud resources
                self.logger.log("Update cluster size", 3)
                new_result = copy.deepcopy(result)
                for j in range(self.system.FaaS_start_index):
                    new_result.reduce_cluster_size(j,self.system)
                # compute the updated cost
                self.logger.log("Compute new cost", 3)
                new_result.objective_function(self.system)
                # update the list of VM numbers according to the new solution
                y_bar = new_result.solution.get_y_bar()
                for j in range(self.system.FaaS_start_index):
                    if y_bar[j] > 0:
                        VM_numbers_random[j] = copy.deepcopy(min(y_bar[j], VM_numbers_random[j]))
            else:
                self.logger.log("The solution is not feasible.")
                new_result = copy.deepcopy(result)
            self.logger.level -= 2

        return result, new_result, (res_parts_random, VM_numbers_random, CL_res_random)

    ## Method to generate a random greedy solution
    #   @param self The object pointer
    #   @return (1) Best Solution.Result before cluster update
    #           (2) Solution.EliteResults object storing the given number of
    #           Solution.Result objects sorted by minimum cost
    #           (4) List of the random parameters
    def run_algorithm(self):

        # initialize the elite set, the best result without cluster update
        # and the lists of random parameters
        elite = EliteResults(self.k_best, Logger(self.logger.stream,
                             self.logger.verbose, self.logger.level + 1))
        best_result_no_update = Result(self.logger)
        # add initial unfeasible sol with inf cost and violation ratio
        elite.elite_results.add(best_result_no_update)
        res_parts_random_list = []
        VM_numbers_random_list = []
        CL_res_random_list = []

        # perform randomized greedy iterations
        self.logger.log("Starting Randomized Greedy procedure", 1)
        self.logger.level += 1
        feasible_sol_found = False
        iteration = 0
        start = time.time()
        lowest_violation = np.inf
        while iteration < self.max_iterations or time.time() - start < self.max_time:
            self.logger.log("Iteration {} --> time: {}, seed: {}".format(iteration, time.time(), self.seed))
            # perform a step
            result, new_result, random_param = self.step()

            if not feasible_sol_found and not new_result.performance[0]:
                if new_result.violation_rate < lowest_violation:
                    lowest_violation = new_result.violation_rate

            else:
                if not feasible_sol_found:
                    feasible_sol_found = True
                    elite = EliteResults(self.k_best, Logger(self.logger.stream,
                                         self.logger.verbose, self.logger.level + 1))
                    best_result_no_update = Result(self.logger)
                    elite.elite_results.add(best_result_no_update)
            # update the results and the lists of random parameters
            elite.add(new_result, feasible_sol_found)
            if result < best_result_no_update:
                best_result_no_update = copy.deepcopy(result)

            # res_parts_random_list.append(random_param[0])
            # VM_numbers_random_list.append(random_param[1])
            # CL_res_random_list.append(random_param[2])
            iteration += 1
        self.logger.level -= 1

        random_params = [res_parts_random_list, VM_numbers_random_list,
                         CL_res_random_list]

        return best_result_no_update, elite, random_params
