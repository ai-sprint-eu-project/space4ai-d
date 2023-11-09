from classes.Algorithm import BaseAlgorithm
from abc import abstractmethod
import numpy as np
import copy
from classes.Solution import Configuration, Result
from classes.PerformanceEvaluators import ServerFarmPE, EdgePE
from classes.Logger import Logger
import sys
import math
import time


## Heuristics
#
# Abstract class used to represent heuristic methods
class BaseHeuristics(BaseAlgorithm):

    ## @var error
    # Object of Logger class, used to print error messages on sys.stderr

    ## @var counter_obj_evaluation
    # A counter to count the number of objective function evaluation

    ## @var verbose
    # Boolean flag to represent if the verbose is needed for logging messages of heuristics

    ## BaseHeuristics class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param keyword Keyword identifying the heuristic methods
    #   @param log Object of Logger.Logger type
    #   @param **kwargs Additional keyword
    def __init__(self, system, keyword, log, **kwargs):
        super().__init__(keyword)
        self.system = system
        self.logger = log
        self.error = Logger(stream=sys.stderr, verbose=1, error=True)
        self.model = self.find_model_to_sort_res()
        self.verbose = True
        self.counter_obj_evaluation = 0


    ## Method to find the performance model of whole system
    #  If at least one of edge or cloud resources is based on machine learning (ML),
    #  we assume the performance model of whole system is based on machine learning model
    def find_model_to_sort_res(self):
        for comp in self.system.performance_models:
            for part in comp:
                for idx, res in enumerate(part):
                    if idx < self.system.FaaS_start_index:
                        if res is not None and not res.keyword.startswith("QT"):
                            return "ML"
        return "QT"

    ## Method to return all the alternative resources of current allocated
    # resource for the given component
    #   @param self The object pointer
    #   @param comp_idx The index of the Graph.Component object
    #   @param part_idx The index of the Graph.Component.Partition object
    #   @param solution The current feasible solution
    #   @return The indices of the alternative resources
    def alternative_resources(self, comp_idx, part_idx, solution):

        # get the assignment matrix
        Y = solution.get_y()

        # get the list of compatible, unused resources
        l = np.greater(self.system.compatibility_matrix[comp_idx][part_idx, :],
                       Y[comp_idx][part_idx, :])
        resource_idxs = np.where(l)[0]

        return resource_idxs

    ## Method to create a solution from a solution file in output file format
    #   @param self The object pointer
    #   @param solution_file A solution file
    #   @return result
    def create_solution_by_file(self, solution_file):
        # read solution json file
        data = self.system.read_solution_file(solution_file)
        if "components" in data.keys():
            # load components info
            C = data["components"]
            Y_hat = []
        else:
            self.error.log("ERROR: no components available in solution file", 1)
            sys.exit(1)
        # loop over all components
        I = len(self.system.components)
        self.logger.log("Start creating the solution by using the file: {}".format(time.time()), 3)
        for i in range(I):
            # get the number of partitions and available resources
            H, J = self.system.compatibility_matrix[i].shape
            # create the empty matrices
            Y_hat.append(np.full((H, J), 0, dtype=int))
        # loop over components
        for c in C:
            if c in self.system.dic_map_com_idx.keys():
                comp_idx = self.system.dic_map_com_idx[c]
            else:
                self.error.log("ERROR: the component does not exist in the system", 1)
                sys.exit(1)
            dep_included = False
            # loop over deployments
            for s in C[c]:
                if s.startswith("s"):
                    dep_included = True
                    part_included = False
                    # loop over partitions
                    for h in C[c][s]:
                        if h.startswith("h"):
                            part_included = True
                            part_idx = self.system.dic_map_part_idx[c][h][1]

                            CL = list(C[c][s][h].keys())[0]
                            res = list(C[c][s][h][CL].keys())[0]
                            res_idx = self.system.dic_map_res_idx[res]
                            if res_idx < self.system.FaaS_start_index:
                                number = C[c][s][h][CL][res]["number"]
                            else:
                                number = 1
                            Y_hat[comp_idx][part_idx][res_idx] = number
                    if not part_included:
                        self.error.log(
                            "ERROR: there is no selected partition for component " + c + " and deployment " + s + " in the solution file",
                            1)
                        sys.exit(1)
            if not dep_included:
                self.error.log(
                    "ERROR: there is no selected deployment for component " + c + " in the solution file", 1)
                sys.exit(1)
        result = Result(self.logger)
        result.solution = Configuration(Y_hat, self.logger)
        self.logger.log("Start check feasibility: {}".format(time.time()))
        performance = result.check_feasibility(self.system)
        self.logger.log("End check feasibility: {}".format(time.time()))
        if performance[0]:
            self.logger.log("Solution is feasible", 3)
            self.logger.log("Compute cost", 3)
            result.objective_function(self.system)

        return result

    ## Method to get all partitions that can be run on FaaS
    #   @param self The object pointer
    #   @param Y_hat Assignment matrix includes all components and partitions assigned to resources
    #   @return A list of partitions allocated to the FaaS including the components index, partition index and resource index
    def get_partitions_with_FaaS(self, Y_hat):

        partitions_with_FaaS = []
        # loop over components
        for comp_idx, comp in enumerate(Y_hat):

            # get the partitions and resources allocated to them

            h_idxs, res_idxs = comp.nonzero()
            # get the indexes of FaaS allocated to the partitions
            res_FaaS_idx = res_idxs[res_idxs >= self.system.FaaS_start_index]
            # if the allocated resources are in FaaS, get the index of partitions and FaaS to add to the output list
            if len(res_FaaS_idx) > 0:

                for i in range(len(res_FaaS_idx)):
                    h_FaaS_idx = comp[:, res_FaaS_idx[i]].nonzero()[0][0]
                    partitions_with_FaaS.append((comp_idx, h_FaaS_idx, res_FaaS_idx[i]))

        return partitions_with_FaaS

    ## Method to get all partitions that is running on resource j
    #   @param self The object pointer
    #   @param Y_hat Assignment matrix includes all components and partitions assigned to resources
    #   @param j The index of resource
    #   @return A list of partitions allocated to resource j including the components index and partition index
    def get_partitions_with_j(self, Y_hat, j):
        partitions_with_j = []
        # loop over components
        for comp_idx, comp in enumerate(Y_hat):
            # get the partitions that are located in resource j
            comps_parts = np.nonzero(comp[:, j])[0]
            # if some partitions are located in j, add them to output list
            if len(comps_parts > 0):

                for comp_part in comps_parts:
                    partitions_with_j.append((comp_idx, comp_part))

        return partitions_with_j

    ## Method to change the input solution to find some neigbors of the current solution by changing the FaaS assignments
    #   @param self The object pointer
    #   @param solution Current solution
    #   @return A list neigbors (new solutions) sorted by cost
    def change_FaaS(self, solution):
        new_sorted_results = None
        new_feasible_results = []
        counter_obj_evaluation = 0
        # call the method to get all partitions located in FaaS
        partitions_with_FaaS = self.get_partitions_with_FaaS(solution.Y_hat)
        # loop over list of partitions in partitions_with_FaaS list
        for comp_part_j in partitions_with_FaaS:
            # get all the alternative resources of the partition
            res_idx = self.alternative_resources(comp_part_j[0], comp_part_j[1], solution)
            # Extract only the FaaS resources from alternative resources
            Faas_res_idx = filter(lambda x: x >= self.system.FaaS_start_index, res_idx)
            # loop over alternative FaaS resources
            for j in Faas_res_idx:
                # get a copy of current solution as a new temprary assignment matrix (Y_hat)
                new_temp_Y_hat = copy.deepcopy(solution.Y_hat)
                # assign the current partition to the new alternative FaaS in new Y_hat
                new_temp_Y_hat[comp_part_j[0]][comp_part_j[1]][j] = 1
                new_temp_Y_hat[comp_part_j[0]][comp_part_j[1]][comp_part_j[2]] = 0
                # create a solution by new Y_hat
                new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                # check the feasibility of new solution
                performance = new_temp_solution.check_feasibility(self.system)
                # if the new solution is feasible, add it to the neighbor result list
                if performance[0]:
                    result = Result(self.logger)
                    result.solution = new_temp_solution
                    result.cost = result.objective_function(self.system)
                    counter_obj_evaluation += 1
                    result.performance = performance
                    new_feasible_results.append(result)
        if len(new_feasible_results) > 0:
            # sort the list of result
            new_sorted_results = sorted(new_feasible_results, key=lambda x: x.cost)
            # new_sorted_solution=[x.solution for x in new_sorted_results]
        # return the list of neibors
        return new_sorted_results, counter_obj_evaluation

    ## Method to get active resources and computationallayers
    #   @param self The object pointer
    #   @param Y_hat Assignment matrix includes all components and partitions assigned to resources
    #   @return (1) A list of resources that are in used in current Y_hat called active resources
    #           (2) A list includes the computational layers of active resources
    def get_active_res_computationallayers(self, Y_hat):

        act_res_idxs = []
        # loop over components
        for comp in Y_hat:
            # get all nodes that are using by the partitions of current component
            h_idxs, res_idxs = comp.nonzero()
            # add the list res to the active resource list
            act_res_idxs.extend(res_idxs)
        # remove the duplicated resource indeces in active resource list
        active_res_idxs = list(set(act_res_idxs))

        # initialize active computational layer list
        act_camputationallayers = []
        # loop over active resource list
        for act_res in active_res_idxs:
            # append the computational layer of current resource to the  active computational layer list
            act_camputationallayers.append(self.system.resources[act_res].CLname)
        # remove the duplicated computational layers in active computational layer list
        active_camputationallayers = list(set(act_camputationallayers))
        return active_res_idxs, active_camputationallayers

    ## Method to sort all nodes increasingly except FaaS by utilization and cost
    #   @param self The object pointer
    #   @param Y_hat Assignment matrix includes all components and partitions assigned to resources
    #   @return 1) The sorted list of resources by utilization and cost, respectively.
    #           Each item of list includes the index, utilization and cost of the resource.
    #           The list is sorted by utilization, but for the nodes with same utilization, it is sorted by cost
    #           2) The sorted list of resources by cost and utilization, respectively.
    #           Each item of list includes the index, utilization and cost of the resource.
    #           The list is sorted by utilization, but for the nodes with same utilization, it is sorted by cost
    def sort_nodes(self, Y_hat):

        # min_utilization=np.inf
        idx_min_U_node = []
        # loop over all alternative resources
        edge = EdgePE()
        cloud = ServerFarmPE()
        for j in range(self.system.cloud_start_index):
            # compute the utilization of current node
            utilization = edge.compute_utilization(j, Y_hat, self.system)
            if not math.isnan(utilization) and utilization > 0:
                # add the information of node to the list includes node index, utilization and cost
                idx_min_U_node.append((j, utilization, self.system.resources[j].cost))

        for j in range(self.system.cloud_start_index, self.system.FaaS_start_index):
            # compute the utilization of current node
            utilization = cloud.compute_utilization(j, Y_hat, self.system)
            if not math.isnan(utilization) and utilization > 0:
                # add the information of node to the list includes node index, utilization and cost
                idx_min_U_node.append((j, utilization, self.system.resources[j].cost))

        # sort the list based on utilization and cost respectively
        sorted_node_by_U_cost = sorted(idx_min_U_node, key=lambda element: (element[1], element[2]))
        # sort the list based on cost and utilization respectively
        sorted_node_by_cost_U = sorted(idx_min_U_node, key=lambda element: (element[2], element[1]))
        # return the index of best alternative
        return sorted_node_by_U_cost, sorted_node_by_cost_U

    ## Method to shuffle all nodes randomly except FaaS
    #  This method ia used if the system is based on ML models
    #   @param self The object pointer
    #   @return The list of resources in random order
    #           Each item of list includes the index, utilization (which is zero) and cost of the resource.

    def shuffle_nodes(self):
        import random
        utilization = 0
        idx_min_U_node = []
        for j in range(self.system.FaaS_start_index):
            idx_min_U_node.append((j, utilization, self.system.resources[j].cost))
        #return random.sample(idx_min_U_node, k=len(idx_min_U_node))
        np.random.shuffle(idx_min_U_node)
        return idx_min_U_node
    ## Method to change the current solution by changing component placement
    #   @param self The object pointer
    #   @param solution Current solution
    #   @param sorting_method indicate the sorting order of nodes.
    #           If sorting_method=0, the list of nodes are sorted by utilization and cost respectively
    #           otherwise the list of nodes are sorted by cost and utilization respectively.
    #   @return A list neigbors (new solutions) sorted by cost
    def change_component_placement(self, solution, sorting_method=0):
        counter_obj_evaluation = 0
        neighbors = []
        new_sorted_results = None
        if self.model == "QT":
            # get a sorted list of nodes' index with their utilization and cost (except FaaS)
            if sorting_method == 0:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[0]
            else:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[1]
        else:
            nodes_sorted_list = self.shuffle_nodes()
        # get resource with maximum utilization as source node
        # first while loop is used for the case that if we cannot find any neighbors by starting with last node of the sorted list as a source node,
        # we try to search to find neigbors by starting from second last node in the list and etc. Continue untile finding neigbors
        j = 1
        # explore nodes_sorted_list until finding at least one neighbor
        while len(neighbors) < 1 and j <= len(nodes_sorted_list):
            # get resource with maximum utilization/cost as source node
            selected_node = len(nodes_sorted_list) - j
            idx_source_node = nodes_sorted_list[selected_node][0]
            # get all partitions located in higest utilization node
            partitions = self.get_partitions_with_j(solution.Y_hat, idx_source_node)
            # get the list of nodes and computational layers in used
            active_res_idxs, active_camputationallayers = self.get_active_res_computationallayers(solution.Y_hat)
            # loop over partitions
            for part in partitions:
                # get all alternative resources of the partitions
                alternative_res_idxs = self.alternative_resources(part[0], part[1], solution)
                # # set a boolean variable to break, if best destination is founded
                # find=False
                i = 0

                # search to find the best campatible resource with lowest utilization for current partition
                # while not find and i<len(nodes_sorted_list)-1 and i<j:
                while i < len(nodes_sorted_list) - 1 and i < j:

                    des_node_idx = nodes_sorted_list[i][0]
                    # Check some conditions to avoid violating the limitation of our problem that says:
                    # Only one node can be in used in each computational layer
                    # So, the destination node can be used only if it is one of running (active) node,
                    # or its computational layer is not an active computational layer

                    if des_node_idx in active_res_idxs or \
                            self.system.resources[des_node_idx].CLname not in active_camputationallayers:
                        if des_node_idx in alternative_res_idxs:

                            # get a copy of current solution as a new temprary assignment matrix (Y_hat)
                            new_temp_Y_hat = copy.deepcopy(solution.Y_hat)
                            # get all partitions running on the destination node
                            partitions_min_U = self.get_partitions_with_j(solution.Y_hat, des_node_idx)
                            # assign the current partition to the new alternative node in new Y_hat with maximume number of its instances
                            new_temp_Y_hat[part[0]][part[1]][idx_source_node] = 0
                            new_temp_Y_hat[part[0]][part[1]][des_node_idx] = self.system.resources[
                                des_node_idx].number

                            if len(partitions_min_U) > 0:
                                # assign the maximume instance number of destination node to the partitions that are running on destination node
                                for part_min in partitions_min_U:
                                    new_temp_Y_hat[part_min[0]][part_min[1]][des_node_idx] = self.system.resources[
                                        des_node_idx].number
                            # creat a solution by new assignment (Y_hat)
                            new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                            # check if new solution is feasible
                            performance = new_temp_solution.check_feasibility(self.system)
                            if performance[0]:
                                # creat new result
                                result = Result(self.logger)
                                result.solution = new_temp_solution
                                # reduce cluster size of source and destination nodes
                                result.reduce_cluster_size(idx_source_node, self.system)
                                result.reduce_cluster_size(des_node_idx, self.system)
                                # compute the cost
                                result.objective_function(self.system)
                                counter_obj_evaluation += 1
                                #result.performance = performance
                                # add new result in neigbor list
                                neighbors.append(result)

                    i += 1
                # if not find:
                #      print("There is no alternative node for partition "+str(part[1]) +" of component "+ str(part[0])+" in current solution." )
            # if some neighbors are founded, sort them by cost and return the list
            if len(neighbors) > 0:
                new_sorted_results = sorted(neighbors, key=lambda x: x.cost)
                # new_sorted_solutions=[x.solution for x in new_sorted_results]

            else:
                # print("No neighbor could be find by changing component placement of source node " +str(idx_source_node))
                new_sorted_results = None
            j += 1

        # if new_sorted_results is None:
        #     print("Any neighbors could not be found by changing component placement for the current solution")
        return new_sorted_results, counter_obj_evaluation

    ## Method to change the current solution by changing resource type
    #   @param self The object pointer
    #   @param solution Current solution
    #   @param sorting_method indicate the sorting order of nodes.
    #           If sorting_method=0, the list of nodes are sorted by utilization and cost respectively
    #           otherwise the list of nodes are sorted by cost and utilization respectively.
    #   @return A list neigbors (new solutions) sorted by cost
    def change_resource_type(self, solution, sorting_method=0):

        counter_obj_evaluation = 0
        neighbors = []
        new_sorted_results = None
        if self.model == "QT":
            # get a sorted list of nodes' index with their utilization and cost (except FaaS)
            if sorting_method == 0:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[0]
            else:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[1]
        else:
            # if the model is not based on M/G/1 queue, sort the resources randomly
            nodes_sorted_list = self.shuffle_nodes()
        i = 1
        while len(neighbors) < 1 and i <= len(nodes_sorted_list):
            # get resource with maximum utilization/cost as source node
            selected_node = len(nodes_sorted_list) - i
            idx_source_node = nodes_sorted_list[selected_node][0]

            # get all partitions located in highest utilization node
            partitions = self.get_partitions_with_j(solution.Y_hat, idx_source_node)
            alternative_res_idxs_parts = []
            # get a list of set of alternative nodes for partitions runing on source node
            for part in partitions:
                alternative_res_idxs_parts.append(set(self.alternative_resources(part[0], part[1], solution)))
            # get the intersection of the alternative nodes of all partitions runing on source node
            if len(alternative_res_idxs_parts) > 0:
                candidate = set.intersection(*alternative_res_idxs_parts)
                candidate_nodes = [i for i in candidate if i < self.system.FaaS_start_index]
            else:
                candidate_nodes = []

            if len(candidate_nodes) > 0:
                # get the list of nodes and computational layers in used
                active_res_idxs, active_camputationallayers = self.get_active_res_computationallayers(
                    solution.Y_hat)
                # for each candidate nodes, move all partitions on it and create new solution
                for des in candidate_nodes:
                    # Check some conditions to avoid violating the limitation of our problem that says:
                    # Only one node can be in used in each computational layer
                    # So, the destination node can be used only if it is one of running node,
                    # or its computational layer is not an active computational layer
                    # or if the source and destination node are located in the same computational layer
                    if des in active_res_idxs or \
                            self.system.resources[des].CLname not in active_camputationallayers or \
                            self.system.resources[des].CLname == self.system.resources[idx_source_node].CLname:
                        new_temp_Y_hat = copy.deepcopy(solution.Y_hat)
                        # get all partitions running on the destination node
                        partitions_on_candidate = self.get_partitions_with_j(solution.Y_hat, des)
                        # assign the maximume instance number of destination node to the partitions that are running on source node
                        for part in partitions:
                            new_temp_Y_hat[part[0]][part[1]][idx_source_node] = 0
                            new_temp_Y_hat[part[0]][part[1]][des] = self.system.resources[des].number
                        if len(partitions_on_candidate) > 0:
                            # assign the maximume instance number of destination node to the partitions that are running on destination node
                            for part_cand in partitions_on_candidate:
                                new_temp_Y_hat[part[0]][part[1]][des] = self.system.resources[des].number
                        # create new solution by new assignment
                        new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                        # check feasibility

                        performance = new_temp_solution.check_feasibility(self.system)

                        if performance[0]:
                            # create a new result
                            result = Result(self.logger)
                            result.solution = new_temp_solution
                            # reduce the cluster size of destination node
                            result.reduce_cluster_size(des, self.system)
                            result.objective_function(self.system)
                            counter_obj_evaluation += 1
                            #new_result.performance = performance
                            # add the new result to the neigbor list
                            neighbors.append(result)

                if len(neighbors) > 0:
                    # sort neighbor list by cost and return the best one
                    new_sorted_results = sorted(neighbors, key=lambda x: x.cost)
                    # new_sorted_solutions=[x.solution for x in new_sorted_results]
            #     else:
            #         print("No neighbor could be find by changing resource "+str(idx_source_node)+" because no feasible solution exists given the shared compatiblie nodes ")

            # else:
            #     print("No neighbor could be find by changing resource "+str(idx_source_node)+" because no shared compatiblie node exists")
            i += 1
        # if  new_sorted_results==None:
        #    print("Any neighbors could not be found by changing resource type for the current solution")
        return new_sorted_results, counter_obj_evaluation

    ## Method to change the current solution by moveing partitions from edge or cloud toFaaS
    #   @param self The object pointer
    #   @param solution Current solution
    #   @param sorting_method indicate the sorting order of nodes.
    #           If sorting_method=0, the list of nodes are sorted by utilization and cost respectively
    #           otherwise the list of nodes are sorted by cost and utilization respectively.
    #   @return A list neigbors (new solutions) sorted by cost
    def move_to_FaaS(self, solution, sorting_method=0):
        counter_obj_evaluation = 0
        neighbors = []
        new_sorted_results = None
        if self.model == "QT":
            # get a sorted list of nodes' index with their utilization and cost (except FaaS)
            if sorting_method == 0:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[0]
            else:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[1]
        else:
            # if the model is not based on M/G/1 queue, sort the resources randomly
            nodes_sorted_list = self.shuffle_nodes()

        # sort FaaS by memory and cost respectively
        sorted_FaaS = self.system.sorted_FaaS_by_memory_cost
        # if we need to sort FaaS by cost and memory respectively, we use self.system.sorted_FaaS_by_cost_memory
        sorted_FaaS_idx = [i[0] for i in sorted_FaaS]
        i = 1
        while len(neighbors) < 1 and i <= len(nodes_sorted_list):
            # get resource with maximum utilization/cost as source node
            selected_node = len(nodes_sorted_list) - i
            idx_source_node = nodes_sorted_list[selected_node][0]

            # get all partitions located in higest utilization node
            partitions = self.get_partitions_with_j(solution.Y_hat, idx_source_node)
            alternative_res_idxs_parts = []
            all_FaaS_compatible = True
            # get a list of alternative FaaS for partitions runing on source node
            for part in partitions:
                x = self.alternative_resources(part[0], part[1], solution)
                FaaS_alternatives = x[x >= self.system.FaaS_start_index]
                if len(FaaS_alternatives) < 1:
                    all_FaaS_compatible = False
                alternative_res_idxs_parts.append(FaaS_alternatives)

            if all_FaaS_compatible and len(alternative_res_idxs_parts) > 0:
                new_temp_Y_hat = copy.deepcopy(solution.Y_hat)
                for idx, part in enumerate(partitions):
                    idx_alternative = [sorted_FaaS_idx.index(j) for j in alternative_res_idxs_parts[idx]]
                    # get the FaaS with maximum memory/cost
                    des = sorted_FaaS_idx[max(idx_alternative)]
                    # alternative_res_idxs_parts[idx] sorted_FaaS
                    new_temp_Y_hat[part[0]][part[1]][idx_source_node] = 0

                    new_temp_Y_hat[part[0]][part[1]][des] = 1
                # create new solution by new assignment
                new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                # check feasibility
                performance = new_temp_solution.check_feasibility(self.system)

                if performance[0]:
                    # create a new result
                    new_result = Result(self.logger)
                    new_result.solution = new_temp_solution
                    new_result.cost = new_result.objective_function(self.system)
                    counter_obj_evaluation += 1
                    new_result.performance = performance
                    # add the new result to the neigbor list
                    neighbors.append(new_result)
            else:

                for idx, part in enumerate(partitions):
                    if len(alternative_res_idxs_parts[idx]) > 0:
                        new_temp_Y_hat = copy.deepcopy(solution.Y_hat)

                        idx_alternative = [sorted_FaaS_idx.index(j) for j in alternative_res_idxs_parts[idx]]
                        des = sorted_FaaS_idx[max(idx_alternative)]
                        # alternative_res_idxs_parts[idx] sorted_FaaS
                        new_temp_Y_hat[part[0]][part[1]][idx_source_node] = 0

                        new_temp_Y_hat[part[0]][part[1]][des] = 1
                        # create new solution by new assignment
                        new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                        # check feasibility
                        performance = new_temp_solution.check_feasibility(self.system)

                        if performance[0]:
                            # create a new result
                            result = Result(self.logger)
                            result.solution = new_temp_solution
                            # reduce the cluster size of destination node
                            result.reduce_cluster_size(idx_source_node, self.system)
                            result.objective_function(self.system)
                            counter_obj_evaluation += 1
                            #new_result.performance = performance
                            # add the new result to the neigbor list
                            neighbors.append(result)
            i += 1

        if len(neighbors) > 0:
            # sort neighbor list by cost and return the best one
            new_sorted_results = sorted(neighbors, key=lambda x: x.cost)
            # new_sorted_solutions=[x.solution for x in new_sorted_results]
        # else:
        #    print("Any neighbors could not be found by moveing to FaaS for the current solution")

        return new_sorted_results, counter_obj_evaluation

    ## Method to move the partitions running on FaaS to the edge/cloud
    #   @param self The object pointer
    #   @param solution Current solution
    #   @param sorting_method indicate the sorting order of nodes.
    #           If sorting_method=0, the list of nodes are sorted by utilization and cost respectively
    #           otherwise the list of nodes are sorted by cost and utilization respectively.
    #   @return A list neigbors (new solutions) sorted by cost
    def move_from_FaaS(self, solution, sorting_method=0):
        counter_obj_evaluation = 0
        neighbors = []
        new_sorted_results = None
        # get a sorted list of nodes' index with their utilization and cost (except FaaS)
        if self.model == "QT":
            # get a sorted list of nodes' index with their utilization and cost (except FaaS)
            if sorting_method == 0:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[0]
            else:
                nodes_sorted_list = self.sort_nodes(solution.Y_hat)[1]
        else:
            # if the model is not based on M/G/1 queue, sort the resources randomly
            nodes_sorted_list = self.shuffle_nodes()
        # call the method to get all partitions located in FaaS
        partitions_with_FaaS = self.get_partitions_with_FaaS(solution.Y_hat)
        # get the list of nodes and computational layers in used
        active_res_idxs, active_camputationallayers = self.get_active_res_computationallayers(solution.Y_hat)
        # loop over partitions
        for part in partitions_with_FaaS:
            # get all alternative resources of the partitions
            alternative_res_idxs = self.alternative_resources(part[0], part[1], solution)
            alternative_res_idxs = alternative_res_idxs[alternative_res_idxs < self.system.FaaS_start_index]
            i = 0
            find = False
            # search to find the best campatible resource with lowest utilization for current partition
            # while not find and i<len(nodes_sorted_list)-1 and i<j:
            while i < len(nodes_sorted_list) - 1 and not find:

                des_node_idx = nodes_sorted_list[i][0]
                # Check some conditions to avoid violating the limitation of our problem that says:
                # Only one node can be in used in each computational layer
                # So, the destination node can be used only if it is one of running (active) node,
                # or its computational layer is not an active computational layer

                if des_node_idx in active_res_idxs or \
                        self.system.resources[des_node_idx].CLname not in active_camputationallayers:
                    if des_node_idx in alternative_res_idxs:

                        # get a copy of current solution as a new temprary assignment matrix (Y_hat)
                        new_temp_Y_hat = copy.deepcopy(solution.Y_hat)
                        # get all partitions running on the destination node
                        partitions_on_des = self.get_partitions_with_j(solution.Y_hat, des_node_idx)
                        # assign the current partition to the new alternative node in new Y_hat with maximume number of its instances
                        new_temp_Y_hat[part[0]][part[1]][part[2]] = 0
                        new_temp_Y_hat[part[0]][part[1]][des_node_idx] = self.system.resources[des_node_idx].number

                        if len(partitions_on_des) > 0:
                            # assign the maximume instance number of destination node to the partitions that are running on destination node
                            for part_des in partitions_on_des:
                                new_temp_Y_hat[part_des[0]][part_des[1]][des_node_idx] = self.system.resources[
                                    des_node_idx].number
                        # creat a solution by new assignment (Y_hat)
                        new_temp_solution = Configuration(new_temp_Y_hat, self.logger)
                        # check if new solution is feasible
                        performance = new_temp_solution.check_feasibility(self.system)
                        if performance[0]:
                            # creat new result
                            result = Result(self.logger)
                            result.solution = new_temp_solution
                            # reduce cluster size of the destination node
                            result.reduce_cluster_size(des_node_idx, self.system)
                            # compute the cost
                            result.objective_function(self.system)
                            counter_obj_evaluation += 1
                            #new_result.performance = performance
                            # add new result in neigbor list
                            neighbors.append(result)
                            find = True

                i += 1

        if len(neighbors) > 0:
            new_sorted_results = sorted(neighbors, key=lambda x: x.cost)
        # if new_sorted_results is None:
        #     print("Any neighbors could not be found by moving from FaaS to edge/cloud for the current solution")
        return new_sorted_results, counter_obj_evaluation

    ## Method to union and sort the set of neighbors came from three methods: change_resource_type, change_component_placement, change_FaaS
    #   @param self The object pointer
    #   @param solution Current solution
    #   @return A list neigbors (new solutions) sorted by cost
    def union_neighbors(self, solution):

        neighborhood = []
        # get the neighbors by changing FaaS configuration
        neighborhood1, counter_obj_evaluation1 = self.change_FaaS(solution)
        # get the neighbors by changing resource type
        neighborhood2, counter_obj_evaluation2 = self.change_resource_type(solution)
        # get the neigbors by changing component placement
        neighborhood3, counter_obj_evaluation3 = self.change_component_placement(solution)
        # get the neigbors by moveing to FaaS
        neighborhood4, counter_obj_evaluation4 = self.move_to_FaaS(solution)
        # get the neigbors by moveing from FaaS to edge/cloud
        neighborhood5, counter_obj_evaluation5 = self.move_from_FaaS(solution)
        counter_obj_evaluation = counter_obj_evaluation1 + counter_obj_evaluation2 + counter_obj_evaluation3 + \
                                 counter_obj_evaluation4 + counter_obj_evaluation5
        # mixe all neigbors
        if neighborhood1 is not None:
            neighborhood.extend(neighborhood1)
        if neighborhood2 is not None:
            neighborhood.extend(neighborhood2)
        if neighborhood3 is not None:
            neighborhood.extend(neighborhood3)
        if neighborhood4 is not None:
            neighborhood.extend(neighborhood4)
        if neighborhood5 is not None:
            neighborhood.extend(neighborhood5)
        # sort the neighbors list by cost
        sorted_neighborhood = sorted(neighborhood, key=lambda x: x.cost)
        # if two solution have the same cost, check if the solutions are the same and drop one of them
        new_sorted_neighborhood = copy.deepcopy(sorted_neighborhood)
        for neighbor_idx in range(len(sorted_neighborhood) - 1):
            if sorted_neighborhood[neighbor_idx].cost == sorted_neighborhood[neighbor_idx + 1].cost:
                if sorted_neighborhood[neighbor_idx].solution == sorted_neighborhood[neighbor_idx + 1].solution:
                    new_sorted_neighborhood.remove(new_sorted_neighborhood[neighbor_idx])

        return new_sorted_neighborhood, counter_obj_evaluation

    ## Method to create the initial solution with largest configuration function (for only FaaS scenario)
    #   @param self The object pointer
    #   @return solution
    def creat_initial_solution_with_largest_conf_fun(self):
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
                candidate_nodes.append(random_num)
            resource_count += len(l.resources)
        for comp in self.system.components:

            # randomly select a deployment for that component
            random_dep = np.random.choice(comp.deployments)

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
                max_cost = 0
                for j_idx in index:
                    if j_idx in range(self.system.cloud_start_index):
                        cost = self.system.resources[j_idx].cost
                    elif j_idx in range(self.system.cloud_start_index, self.system.FaaS_start_index):
                        cost = self.system.resources[j_idx].cost
                    #
                    # compute the cost of FaaS and transition cost if not using SCAR
                    elif j_idx in range(self.system.FaaS_start_index, J):

                        cost = self.system.resources[j_idx].cost * self.system.components[
                            i].comp_Lambda * self.system.T

                    if cost > max_cost:
                        max_cost = copy.deepcopy(cost)
                        j_largest = j_idx

                y[i][h_idx, j_largest] = 1
                y_hat[i][h_idx, j_largest] = 1
                # if the partition is the last partition (i.e., its successor
                # is the successor of the component), update the size of
                # data transferred between the components
                if self.system.graph.G.succ[comp.name] != {}:
                    if part.Next == list(self.system.graph.G.succ[comp.name].keys())[0]:
                        self.system.graph.G[comp.name][part.Next]["data_size"] = part.data_size
        solution = Configuration(y_hat, self.logger)

        feasible = solution.check_feasibility(self.system)

        if feasible:
            new_solution = solution

        else:
            new_solution = None
        return new_solution

    ## Method to evaluate the object performance through the class predictor
    #   @param self The object pointer
    #   @param **features Model features
    #   @return Predicted response time
    @abstractmethod
    def run_algorithm(self, **parameters):
        pass

    ## Operator to convert a BaseHeuristic object into a string
    #   @param self The object pointer
    def __str__(self):
        s = '"Heuristic":"{}"'. \
            format(self.keyword)
        return s

## BinarySearch calss which uses binary search to find the maximum feasible Lambda based on a given solution
class BinarySearch(BaseHeuristics):

    ## BinarySearch class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    #   @param system_file A system json file
    #   @param solution_file A solution json file
    #   @param log Object of Logger.Logger type
    def __init__(self, system, system_file, solution_file, log=Logger(), **kwargs):
        BaseHeuristics.__init__(self, system, "BinarySearch", log)
        self.system_file = system_file
        self.solution_file = solution_file


    ## Method to increase the number of resources allocated to a partition to max number
    #   @param self The object pointer
    #   @param solution The current feasible solution
    #   @return The assignment matrix Y_hat
    def increase_number_of_resource(self, solution):
        # Loop over all resources but FaaS
        for res_idx in range(self.system.FaaS_start_index):
            # get all components' index and partitions' index that are running j
            partitions_with_j = self.get_partitions_with_j(solution.Y_hat, res_idx)
            # assign the maximum number of the resource to the partitions running on the resource
            for comp_part in partitions_with_j:
                solution.Y_hat[comp_part[0]][comp_part[1], res_idx] = self.system.resources[res_idx].number

        return solution.Y_hat

    ## Method to run the algorithm
    #   @param self The object pointer
    #   @param upper_bound_lambda The maximum Lambda
    #   @param epsilon The gap between highest feasible Lambda and lowes unfeasible Lambda
    #   @param **parameters algorithm parameters
    #   @return result and highest feasible lambda
    def run_algorithm(self, upper_bound_lambda, epsilon, Y_hat=None,  **parameters):
        from classes.System import System

        initial_lambda = self.system.Lambda
        self.logger.log("Start binary search to find max feasible lambda under maximum configuration.", 3)
        if Y_hat is None:
            result = self.create_solution_by_file(self.solution_file)
        else:
            result = Result(self.logger)
            result.solution = Configuration(Y_hat, self.logger)
        #Y_hat = self.increase_number_of_resource(result.solution)
        #Y_hat = result.solution.Y_hat
        eps = np.inf
        lowest_unfeasible_lambda = upper_bound_lambda
        highest_feasible_lambda = self.system.Lambda
        while eps > epsilon:
            self.logger.log("Start check feasibility: {}".format(time.time()), 3)
            performance = result.check_feasibility(self.system)
            self.logger.log("End check feasibility: {}".format(time.time()), 3)
            if performance[0]:
                next_lambda = (lowest_unfeasible_lambda + self.system.Lambda) / 2
                highest_feasible_lambda = self.system.Lambda
                self.system = System(system_file=self.system_file, Lambda=next_lambda, log=self.logger)

            else:
                if self.system.Lambda == initial_lambda:
                    self.error.log("ERROR: The solution with initial Lambda and maximum number of resources is not feasible")
                    sys.exit(1)
                else:
                    next_lambda = (highest_feasible_lambda + self.system.Lambda) / 2
                    lowest_unfeasible_lambda = self.system.Lambda
                    self.system = System(system_file=self.system_file, Lambda=next_lambda, log=self.logger)

            eps = abs(lowest_unfeasible_lambda - highest_feasible_lambda)

        self.system = System(system_file=self.system_file, Lambda=highest_feasible_lambda, log=self.logger)
        result.check_feasibility(self.system)
        result.objective_function(self.system)
        return result, highest_feasible_lambda


