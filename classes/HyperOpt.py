from classes.Algorithm import BaseAlgorithm
import numpy as np
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK, STATUS_FAIL, rand
from hyperopt.fmin import generate_trials_to_calculate
from classes.Solution import Configuration
import time


class HyperOpt(BaseAlgorithm):

    ## HyperOpt class constructor
    #   @param self The object pointer
    #   @param system A System.System object
    def __init__(self, system):
        self.system = system

    ## The objective function of HyperOpt
    #   @param self The object pointer
    #   @param args All arguments with their search space
    #   @return a solution found by HyperOpt
    def objective(self, args):

        # get the search space of all parameters
        resource_random_list, deployment_random_list, VM_number_random_list, prob_res_selection_dep_list = args

        # start to create Y_hat from the search space of parameters
        I = len(self.system.components)
        J = len(self.system.resources)
        y_hat = []
        y = []
        for i in range(I):
            H, J = self.system.compatibility_matrix[i].shape
            # initialize Y_hat, y
            y_hat.append(np.full((H, J), 0, dtype=int))
            y.append(np.full((H, J), 0, dtype=int))

        candidate_nodes = []
        resource_count = 0
        # loop over all computational layers

        for idx, l in enumerate(self.system.CLs):
            # select all nodes in FaaS layers
            if resource_count >= self.system.FaaS_start_index:
                random_num = l.resources
                candidate_nodes.extend(random_num)
            # set a node in other layers based on what HypetOpt selected
            else:
                if resource_random_list[idx] != np.inf:
                    candidate_nodes.append(l.resources[resource_random_list[idx]])
            resource_count += len(l.resources)

        for comp_idx, comp in enumerate(self.system.components):
            # set a deployment for each component based on what HypetOpt selected
            random_dep = comp.deployments[deployment_random_list[comp_idx]]

            h = 0

            # for part_idx, part in enumerate(random_dep.partitions_indices):
            for part_idx in random_dep.partitions_indices:
                part = comp.partitions[part_idx]
                # get the indices of the component and the deployment selected by HypetOpt
                i = self.system.dic_map_part_idx[comp.name][part.name][0]
                # get the indices of compatible resources and compute the
                # intersection with the selected resources in each
                # computational layer

                h_idx = self.system.dic_map_part_idx[comp.name][part.name][1]
                if max(max(prob_res_selection_dep_list)) <= 1:
                    idx = np.nonzero(self.system.compatibility_matrix[i][h_idx, :])[0]
                    # extract a resource index in the intersection
                    index = list(set(candidate_nodes).intersection(idx))
                    prob = 1 / len(index)
                    step = 0
                    h = random_dep.partitions_indices.index(h_idx)
                    rn = prob_res_selection_dep_list[comp_idx][h]

                    for r in np.arange(0, 1, prob):
                        if rn > r and rn <= r + prob:
                            j = index[step]

                        else:
                            step += 1
                else:
                    h = random_dep.partitions_indices.index(h_idx)
                    j = int(prob_res_selection_dep_list[comp_idx][h])
                y[i][h_idx][j] = 1
                y_hat[i][h_idx][j] = 1
                # if the partition is the last partition (i.e., its successor
                # is the successor of the component), update the size of
                # data transferred between the components
                if self.system.graph.G.succ[comp.name] != {}:

                    # if part.Next == list(self.system.graph.G.succ[comp.name].keys())[0]:
                    if part.Next == list(self.system.graph.G.succ[comp.name].keys()):
                        for next_idx in range(len(part.Next)):
                            self.system.graph.G[comp.name][part.Next[next_idx]]["data_size"] = part.data_size[next_idx]

        # check if the system dosent have FaaS
        if self.system.FaaS_start_index != float("inf"):
            edge_VM = self.system.FaaS_start_index
        else:
            edge_VM = J
        # randomly generate the number of resources that can be assigned
        # to the partitions that run on that resource
        for j in range(edge_VM):

            # loop over components
            for i in range(I):
                H = self.system.compatibility_matrix[i].shape[0]
                for h in range(H):
                    if y[i][h][j] > 0:
                        if max(max(prob_res_selection_dep_list)) <= 1:
                            y_hat[i][h][j] = y[i][h][j] * (VM_number_random_list[j] + 1)
                        else:
                            y_hat[i][h][j] = y[i][h][j] * (VM_number_random_list[j])
        solution = Configuration(y_hat)

        flag, primary_paths_performance, primary_components_performance = solution.check_feasibility(self.system)

        if flag:
            costs = solution.objective_function(self.system)
            return {'loss': costs,
                    'time': time.time(),
                    'status': STATUS_OK}
        else:

            return {'status': STATUS_FAIL,
                    'time': time.time(),
                    'exception': "inf"}

    ## Random optimization function by HyperOpt
    #   @param self The object pointer
    #   @param seed Seed for random number generation
    #   @param iteration_number The iteration number for HyperOpt
    #   @param vals_list The list of value to feed the result of RandomGreedy to HyperOpt
    #   @return the best cost and the best solution found by HyperOpt
    def random_hyperopt(self, seed, max_time, vals_list=[]):
        # np.random.seed(int(time.time()))
        # set the seed for replicability
        # os.environ['HYPEROPT_FMIN_SEED'] = "1"#str(np.random.randint(1,1000))
        # Create search spaces for all random variable by defining a dictionary for each of them
        resource_random_list = []
        for idx, l in enumerate(self.system.CLs):
            # Create search spaces for all computational layer except the last one with FaaS
            if l != list(self.system.CLs)[-1]:
                resource_random_list.append(hp.randint("res" + str(idx), len(l.resources)))

        deployment_random_list = []
        prob_res_selection_dep_list = []
        # Create search spaces for all random variable which is needed for components
        for idx, comp in enumerate(self.system.components):
            max_part = 0
            res_random = []
            # Create search spaces for deployments
            deployment_random_list.append(hp.randint("dep" + str(idx), len(comp.deployments)))
            # random_dep=comp.deployments[random_num]
            for dep in comp.deployments:
                if max_part < len(list(dep.partitions_indices)):
                    max_part = len(list(dep.partitions_indices))
            # Create search spaces for resources of partitions
            for i in range(max_part):
                res_random.append(hp.uniform("com_res" + str(idx) + str(i), 0, 1))
            prob_res_selection_dep_list.append(res_random)
        VM_number_random_list = []
        # Create search spaces for determining VM number
        for j in range(self.system.FaaS_start_index):
            VM_number_random_list.append(hp.randint("VM" + str(j), self.system.resources[j].number))

        # creat a list of search space including all variables
        space1 = resource_random_list
        space2 = deployment_random_list
        space3 = VM_number_random_list
        space4 = prob_res_selection_dep_list

        space = [space1, space2, space3, space4]
        # create trails to search in search spaces
        trials = Trials()
        best_cost = 0
        # if we need to use Spark for parallelisation
        # trials = SparkTrials(parallelism=4)

        # if there is some result from RandomGreedy to feed to HyperOpt
        if len(vals_list) > 0:
            trials = generate_trials_to_calculate(vals_list)

        # set seed
        rstate = np.random.default_rng(seed)
        try:
            # run fmin method to search and find the best solution
            best = fmin(fn=self.objective, space=space, algo=rand.suggest, trials=trials, timeout=max_time,
                        rstate=rstate)

        except:

            best_cost = float("inf")

        # check if HyperOpt could find solution
        if best_cost == float("inf"):
            # if HyperOpt cannot find any feasible solution
            solution = None
        else:
            # if HyperOp find a feasible solution extract the solution from fmin output
            best_cost = trials.best_trial["result"]["loss"]
            cost, solution = self.extract_HyperOpt_result(best)

        return best_cost, solution

    ## Method to extract the best solution of HperOpt and converting it to Y_hat
    #   @param self The object pointer
    #   @param best The output of fmin method
    #   @return the best cost and the best solution found by HyperOpt
    def extract_HyperOpt_result(self, best):

        I = len(self.system.components)
        J = len(self.system.resources)
        y_hat = []
        y = []
        # initialize Y_hat and y by 0
        for i in range(I):
            H, J = self.system.compatibility_matrix[i].shape
            y_hat.append(np.full((H, J), 0, dtype=int))
            y.append(np.full((H, J), 0, dtype=int))

        # initialize the list of selected resources by best solution of HyperOpt
        resource_random_list = []
        # extract the selected resources by best solution of HyperOpt in CLs
        candidate_nodes = []
        for idx, l in enumerate(self.system.CLs):
            if l == list(self.system.CLs)[-1]:
                random_num = l.resources
                candidate_nodes.extend(random_num)
            else:
                if str(best["res" + str(idx)]).isdigit():
                    candidate_nodes.append(l.resources[best["res" + str(idx)]])
                    resource_random_list.append(best["res" + str(idx)])

        resources = [v for k, v in best.items() if 'com_res' in k]
        # extract the selected deployments of components by best solution of HyperOpt
        for comp_idx, comp in enumerate(self.system.components):

            random_dep = comp.deployments[best["dep" + str(comp_idx)]]
            # deployment_random_list.append(best["dep"+str(idx)])
            h = 0
            for part_idx in random_dep.partitions_indices:
                part = comp.partitions[part_idx]
                # pick the selected compatible resources by the best solution of HperOpt for each partition
                #  and set Y_hat according to it
                i = self.system.dic_map_part_idx[comp.name][part.name][0]
                h_idx = self.system.dic_map_part_idx[comp.name][part.name][1]
                if max(resources) <= 1:
                    idx = np.nonzero(self.system.compatibility_matrix[i][h_idx, :])[0]
                    index = list(set(candidate_nodes).intersection(idx))
                    prob = 1 / len(index)
                    step = 0
                    h = random_dep.partitions_indices.index(h_idx)
                    rn = best["com_res" + str(comp_idx) + str(h)]
                    for r in np.arange(0, 1, prob):
                        if rn > r and rn <= r + prob:
                            j = index[step]

                        else:
                            step += 1
                else:
                    h = random_dep.partitions_indices.index(h_idx)
                    j = int(best["com_res" + str(comp_idx) + str(h)])
                y[i][h_idx][j] = 1
                y_hat[i][h_idx][j] = 1

                if self.system.graph.G.succ[comp.name] != {}:
                    if part.Next == list(self.system.graph.G.succ[comp.name].keys()):

                        for next_idx in range(len(part.Next)):
                            self.system.graph.G[comp.name][part.Next[next_idx]]["data_size"] = part.data_size[next_idx]
        # pick the number of VM selected by best solution of HyperOpt and set it in Y_hat
        if self.system.FaaS_start_index != float("inf"):
            edge_VM = self.system.FaaS_start_index
        else:
            edge_VM = J
        for j in range(edge_VM):
            for i in range(I):
                H = self.system.compatibility_matrix[i].shape[0]
                for h in range(H):
                    if y[i][h][j] > 0:
                        if max(resources) <= 1:
                            y_hat[i][h][j] = y[i][h][j] * (best["VM" + str(j)] + 1)
                        else:
                            y_hat[i][h][j] = y[i][h][j] * (best["VM" + str(j)])

        # create the solution by new Y_hat extracted by the best solution of HyperOpt
        solution = Configuration(y_hat)
        # compute cost
        cost = solution.objective_function(self.system)

        return cost, solution

    ## Method to create the trials according to solutions of random greedy to feed HyperOpt
    #   @param self The object pointer
    #   @param solutions The solutions of random greedy
    #   @param res_parts_random_list The lists of random parameters needed to select compatible resources assigned to partitions
    #   @param VM_numbers_random_list The list of random number selected by random greedy
    #   @param CL_res_random_list The list of randomly selected resources in CLs by random greedy
    #   @return a list of values to feed HyperOpt
    def creat_trials_by_RandomGreedy(self, solutions, res_parts_random_list,
                                     VM_numbers_random_list, CL_res_random_list):

        # Initialize the list of values to feed HyperOpt
        vals_list = []
        # For all solutions found by random greedy, create the parameters that HyperOpt needs to create the same solutions
        for solution_idx, solution in enumerate(solutions):

            vals = {}
            com_res = {}
            dep = {}
            # initialize the resources assigned to parts by random value,
            # it is necessary for HyperOpt to has the space of all parameters even if the deployment of the partition
            # is not selected by random greedy. The deployments selected by random greedy
            # will assigned to the same resources as random greedy while the others will assign to resources randomly.
            for idx, comp in enumerate(self.system.components):
                max_part = 0

                for dep in comp.deployments:
                    if max_part < len(list(dep.partitions_indices)):
                        max_part = len(list(dep.partitions_indices))
                for i in range(max_part):
                    vals["com_res" + str(idx) + str(i)] = np.random.random()

            # for each component and deployment, set the random parameters selected by random greedy
            for comp_idx, y in enumerate(solution.Y_hat):

                H, J = y.shape
                for h in range(H):

                    resource_idx = np.nonzero(y[h, :])[0]
                    if len(resource_idx) > 0:
                        if solution_idx == 2 and comp_idx == 2:
                            x = 1
                        for dep_idx, dep in enumerate(self.system.components[comp_idx].deployments):
                            if h in dep.partitions_indices:

                                vals["dep" + str(comp_idx)] = dep_idx
                                try:
                                    vals["com_res" + str(comp_idx) + str(dep.partitions_indices.index(h))] = \
                                    res_parts_random_list[solution_idx][comp_idx][dep.partitions_indices.index(h)]
                                except:
                                    print("solution_idx: " + str(solution_idx) + " comp_idx:" + str(
                                        comp_idx) + " part_idx:" + str(dep_idx))
            # set the selected node selected by random greedy
            for idx, l in enumerate(CL_res_random_list[solution_idx]):
                vals["res" + str(idx)] = l
            # set the number of VM selected by random greedy
            if self.system.FaaS_start_index != float("inf"):
                edge_VM = self.system.FaaS_start_index
            else:
                edge_VM = J
            for j in range(edge_VM):

                max_number = 0
                for i in range(len(solution.Y_hat)):
                    if max(solution.Y_hat[i][:, j]) > max_number:
                        max_number = max(solution.Y_hat[i][:, j])

                if max_number > 0:
                    vals["VM" + str(j)] = max_number - 1
                else:
                    vals["VM" + str(j)] = VM_numbers_random_list[solution_idx][j]

            vals_list.append(vals)
        return vals_list

    ## Method to create the trials according to solutions of a heuristic to feed HyperOpt
    #   @param self The object pointer
    #   @param solutions The solutions of random greedy
    #   @param res_parts_random_list The lists of random parameters needed to select compatible resources assigned to partitions
    #   @param VM_numbers_random_list The list of random number selected by random greedy
    #   @param CL_res_random_list The list of randomly selected resources in CLs by random greedy
    #   @return a list of values to feed HyperOpt
    def creat_trials_by_Heuristic(self, solutions):

        # Initialize the list of values to feed HyperOpt
        vals_list = []
        # For all solutions found by random greedy, create the parameters that HyperOpt needs to create the same solutions
        for solution_idx, solution in enumerate(solutions):

            vals = {}
            com_res = {}
            dep = {}
            selected_res_list = []
            number_of_vms_list = []
            y_bar = solution.get_y_bar()
            for idx, comp in enumerate(self.system.components):
                max_part = 0

                for dep in comp.deployments:
                    if max_part < len(list(dep.partitions_indices)):
                        max_part = len(list(dep.partitions_indices))
                for i in range(max_part):
                    vals["com_res" + str(idx) + str(i)] = 1
            for comp_idx, y in enumerate(solution.Y_hat):
                H, J = y.shape
                for h in range(H):

                    resource_idx = np.nonzero(y[h, :])[0]
                    if len(resource_idx) > 0:
                        selected_res_list.append(resource_idx[0])
                        number_of_vms_list.append(y_bar[resource_idx[0]])
                        for dep_idx, dep in enumerate(self.system.components[comp_idx].deployments):
                            if h in dep.partitions_indices:
                                vals["dep" + str(comp_idx)] = dep_idx
                                compatible_res_idx = np.nonzero(self.system.compatibility_matrix[comp_idx][h, :])[0]

                                h_idx = dep.partitions_indices.index(h)
                                vals["com_res" + str(comp_idx) + str(h_idx)] = resource_idx[0]

            if self.system.FaaS_start_index != float("inf"):
                edge_VM = self.system.FaaS_start_index
            else:
                edge_VM = J
            for j in range(edge_VM):
                vals["VM" + str(j)] = np.random.randint(0, self.system.resources[j].number)

            for l in range(len(self.system.CLs)):
                flag = False
                res_list = self.system.CLs[l].resources
                for j in res_list:  # zip(selected_res_list,number_of_vms_list):
                    if j in selected_res_list:
                        flag = True
                        vals["res" + str(l)] = self.system.CLs[l].resources.index(j)
                        idx = selected_res_list.index(j)
                        vals["VM" + str(j)] = number_of_vms_list[idx]
                        break
                if flag == False:
                    vals["res" + str(l)] = np.inf
            # initialize the resources assigned to parts by random value,
            # it is necessary for HyperOpt to has the space of all parameters even if the deployment of the partition
            # is not selected by random greedy. The deployments selected by random greedy
            # will assigned to the same resources as random greedy while the others will assign to resources randomly.

            vals_list.append(vals)
        return vals_list

