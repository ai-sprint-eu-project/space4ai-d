from external import space4ai_logger

from classes.PerformanceEvaluators import SystemPerformanceEvaluator, ServerFarmPE, EdgePE
import numpy as np
import itertools
import json
from datetime import datetime
from uuid import uuid4
from sortedcontainers import SortedList
import sys
import math
import pathlib
from operator import attrgetter
import copy

## Configuration
class Configuration:
    
    ## @var Y_hat
    # List of 2D numpy arrays storing the number of Resources.Resource 
    # assigned to each Graph.Component.Partition
    
    ## @var local_slack_value
    # Slack values related to Constraints.LocalConstraints
    
    ## @var global_slack_value
    # Slack value related to Constraints.GlobalConstraints
    
    ## @var logger
    # Object of Logger type, used to print general messages
   

    ## Configuration class constructor
    #   @param self The object pointer
    #   @param Y_hat List of 2D numpy arrays storing the number of 
    #                Resources.Resource assigned to each 
    #                Graph.Component.Partition
    #   @param log Object of Logger type
    def __init__(
            self, Y_hat, 
            log=space4ai_logger.Logger(name="SPACE4AI-D-Configuration")
        ):
        self.Y_hat = Y_hat
        self.local_slack_value = np.full(len(self.Y_hat), np.inf, 
                                         dtype = float)
        self.global_slack_value = None
        self.logger = log
    
    ## Method to define equality of two solution
    #   @param self The object pointer
    #   @param solution The other solution to compare with current object
    def __eq__(self,solution):
        equality = []
        # compare the equality of assignment for each component
        for i, j in zip(self.Y_hat, solution.Y_hat):
            equality.append(np.array_equal(i, j))
        return all(equality)
    
    ## Method to get information about the used resources
    #   @param self The object pointer
    #   @return 1D numpy array whose j-th element is 1 if resource j is used
    def get_x(self):
        J = self.Y_hat[0].shape[1]
        x = np.full(J, 0, dtype = int)
        for i in range(len(self.Y_hat)):
            x[self.Y_hat[i].sum(axis=0) > 0] = 1
        return x
    
    
    ## Method to get the list of 2D binary numpy arrays storing information 
    # about the resources used to run each Graph.Component.Partition
    def get_y(self):
        Y = []
        for i in range(len(self.Y_hat)):
            Y.append(np.array(self.Y_hat[i] > 0, dtype = int))
        return Y
    
    
    ## Method to get the maximum number of used resources of each type
    #   @param self The object pointer
    #    @return 1D numpy array whose j-th element denotes the maximum number 
    #            of used resources of type j
    def get_y_bar(self):
        y_max = []
        for i in range(len(self.Y_hat)):
            y_max.append(np.array(self.Y_hat[i].max(axis=0), dtype=int))
        y_bar = [max(i) for i in itertools.zip_longest(*y_max, fillvalue=0)]
        return np.array(y_bar)
    
   
    ## Method to check if the preliminary constraints are satisfied
    #   @param self The object pointer
    #   @param compatibility_matrix Compatibility matrix
    #   @param resource_number 1D numpy array storing the number of each 
    #                          resource
    #   @return True if the preliminary constraints are satisfied
    def preliminary_constraints_check_assignments(self, compatibility_matrix, 
                                                  resource_number):
        feasible = True
        i = 0
        I = len(self.Y_hat)
        
        # loop over all components until an infeasible assignment is found
        while i < I and feasible:
            
            # check that each component partition is assigned to exactly one 
            # resource
            if all(np.count_nonzero(row) == 1 for row in self.Y_hat[i]):
                # convert y_hat to y (binary)
                y = np.array(self.Y_hat[i] > 0, dtype = int)
                
                # check that only compatible resources are assigned to the 
                # component partitions
                if np.all(np.less_equal(y, compatibility_matrix[i])):
                    
                    # check that the number of resources assigned to each 
                    # component partition is at most equal to the number of 
                    # available resources of that type
                    if any(self.Y_hat.max(axis=0)[0:resource_number.shape[0]]>resource_number):
                        feasible = False
                else:
                    feasible = False
            else:
                    feasible = False
            
            # increment the component index
            i += 1
        
        return feasible       
        
    
    ## Method to check if memory constraints of all Resources.Resource 
    # objects are satisfied
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return True if the constraints are satisfied
    def memory_constraints_check(self, S):
        
        # create y from y_hat
        y = self.get_y()
       
        # for each resource, check if the sum of the memory requirements of 
        # all component partitions assigned to it is greater than the maximum 
        # capacity of the resource
        feasible = True
        J = len(S.resources)
        j = 0
        while j < J and feasible:
            memory = 0
            for i, c in zip(y, S.compatibility_matrix_memory):
                memory += (i[:,j] * np.array(c[:,j])).sum(axis=0)
                #memory += (i[:,j] * np.array(list(h.memory for h in c.partitions))).sum(axis=0)
                if memory > S.resources[j].memory:
                    feasible = False
            j += 1
        
        return feasible
    
    
    ## Method to check that, if a Graph.Component.Partition object is executed
    # on a Resources.VirtualMachine or a Resources.FaaS, all its successors
    # are not executed on Resources.EdgeNode objects (assignments cannot move
    # back from cloud to edge)
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return True if the constraint is satisfied
    def move_backward_check(self, S):
        feasible = True
        source_nodes= [node[0] for node in S.graph.G.in_degree if node[1]==0]
        visited={node:False for node in S.graph.G.nodes}
        Queue=source_nodes
        while Queue:
            last_part_res=-1
            current_node=Queue.pop(0)
            comp_idx=S.dic_map_com_idx[current_node]
            comp_pred_list=list(S.graph.G.pred[current_node])
            if len(comp_pred_list)>0:
                for comp_pred in comp_pred_list:
                    comp_pred_idx=S.dic_map_com_idx[comp_pred]
                    if len(np.nonzero(self.Y_hat[comp_pred_idx])[0])>0:
                        last_h_idx=np.nonzero(self.Y_hat[comp_pred_idx])[0][-1]
                        last_h_res = np.nonzero(self.Y_hat[comp_pred_idx][last_h_idx,:])[0][0]
                        if last_h_res >= S.cloud_start_index:
                            last_part_res=last_h_res

            # loop over all partitions in the deployment
            for y in self.Y_hat[comp_idx]:
                h = np.nonzero(y)
                if np.size(h) > 0:
                    if last_part_res >= S.cloud_start_index:
                        if h[0][0] < S.cloud_start_index:
                            feasible = False
                    last_part_res = h[0][0]

            visited[current_node]=True
            for node in S.graph.G.neighbors(current_node):
                if not visited[node]:
                    if node not in Queue:
                        Queue.append(node)

        return feasible
    
    
    ## Method to check that only a single Graph.Component.Partition object 
    # is assigned to a Resources.Resource whenever the corresponding 
    # PerformanceModels.BasePerformanceModel does not support co-location, 
    # and that the Resources.Resource object utilization does not exceed 1 
    # if the co-location is admissible
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return True if the assignment is feasible
    def performance_assignment_check(self, S):
        
        feasible = True
        
        # matrix size
        I = len(self.Y_hat)
        
        # loop over all resources
        j = 0
        while j < S.FaaS_start_index and feasible:
            
            # number of partitions assigned to the current resource
            count_j = 0
            colocation_allowed = True
            
            # loop over all components
            i = 0
            while i < I and feasible:
                
                # loop over all partitions
                h = 0
                while h < self.Y_hat[i].shape[0] and feasible:
                    
                    # check if the partition is deployed on resource j
                    if self.Y_hat[i][h,j] > 0:
                        
                        # increment counter
                        count_j += 1
                        
                        # check if the corresponding performance model allows
                        # co-location
                        if not S.performance_models[i][h][j].allows_colocation:
                            colocation_allowed = False
                            
                            # if co-location is not allowed but more than one 
                            # partition is deployed on j, the solution is not 
                            # feasible
                            if not colocation_allowed and count_j > 1:
                                feasible = False
                    h += 1
                 
                # if co-location is not allowed but more than one partition 
                # is deployed on j, the solution is not feasible
                if not colocation_allowed and count_j > 1:
                    feasible = False
                
                i += 1
                
            # if more than one partition is deployed on j
            if count_j > 1:
                # if co-location is not allowed, the solution is not feasible
                if not colocation_allowed:
                    feasible = False
                else:
                    # otherwise, we must check the device utilization
                    if j < S.cloud_start_index:
                        model = EdgePE()
                    else:
                        model = ServerFarmPE()
                    utilization = model.compute_utilization(j, self.Y_hat, S)
                    if utilization >= 1:
                        feasible = False
            
            j += 1
                        
        return feasible


    ## Method to check the feasibility of the current configuration
    #   @param self The object pointer
    #   @param S A System.System object
    def check_feasibility(self, S):

        # define status of components and paths response times and constraints
        I = len(S.components)
        components_performance = [[True, np.infty]] * I
        paths_performance = []
        
        # check if the assignments are compatible with the performance models 
        # in terms of partitions co-location / resources utilization
        self.logger.log("Co-location / Utilization constraints check", 4)
        feasible = self.performance_assignment_check(S)
       
        if feasible:
            # check if the memory constraints are satisfied
            self.logger.log("Memory constraints check", 4)
            feasible = self.memory_constraints_check(S)

            if feasible:
                # check if the cloud placement constraint is satisfied
                self.logger.log("Cloud placement constraint check", 4)
                feasible = self.move_backward_check(S)

                if feasible:
                    # check if all local constraints are satisfied
                    self.logger.log("Local constraints check", 4)
                    for LC in S.local_constraints:
                        i = LC.component_idx
                        components_performance[i] = LC.check_feasibility(S, self)
                        feasible = feasible and components_performance[i][0]
                    
                    if feasible:
                        self.logger.log("Global constraints check", 4)
                        # check global constraints
                        for GC in S.global_constraints:
                            paths_performance.append(GC.check_feasibility(S, self))
                            feasible = feasible and paths_performance[-1][0]

        if not feasible:
            self.logger.log("Unfeasible", 4)
        
        return feasible, paths_performance, components_performance

    
    ## Method return all components' (partitions') response time 
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return Performances A list of tuple includs partition index, the corresponding resource index and performance
    def all_response_times(self, S):
        Performances=[]
       
        for component_idx in range(len(self.Y_hat)):
            j=np.nonzero(self.Y_hat[component_idx])
            # loop over all partitions
            self.logger.log("Evaluating partition response times", 6)
            for h in range(len(j[0])):
                r_idx=j[1][h]
                p_idx=j[0][h]
                if r_idx < S.FaaS_start_index:

                    PM = S.performance_models[component_idx][p_idx][r_idx]
                    features = PM.get_features(c_idx=component_idx, p_idx=p_idx,
                                               r_idx=r_idx, S=S, Y_hat=self.Y_hat)
                    p = PM.predict(**features)
                    self.logger.log("features: {}".format(features), 7)
                else:

                    p = S.demand_matrix[component_idx][p_idx,r_idx]
                    

                Performances.append((component_idx,p_idx,p))
        return Performances
    
    ## Method return all constraints evaluation
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return 1) local_constraints_performance: A list of tuples, includes the index of component related to local constraint,
    #                                          a boolean value that indicates if the constaint is feasible and the performance
    #           2)global_constraints_performance: A list of tuples, includes path name, feasibility of path and its performance
    def all_constraints_evaluation(self, S):
        
        local_constraints_performance=[] 
        for LC in S.local_constraints:
            i = LC.component_idx
            component_performance = LC.check_feasibility(S, self)
            local_constraints_performance.append((i,component_performance[0],component_performance[1]))
        
        self.logger.log("Global constraints check", 4)
        # check global constraints
        global_constraints_performance=[]
        
        for GC in S.global_constraints:
            feassible, Sum=GC.check_feasibility(S, self)
            global_constraints_performance.append((GC.path_name,feassible, Sum ))
          
        return local_constraints_performance, global_constraints_performance
    
    ## Method to compute the cost of a feasible solution
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return total cost
    def objective_function(self, S):
        
        J = len(S.resources)
        
        # get information about the used resources and the max number of 
        # used resources of each type
        x = self.get_x()   
        y_bar = self.get_y_bar()
        
        # compute costs
        costs = []
        # compute cost of edge
        for j in range(S.cloud_start_index):
            costs.append(S.resources[j].cost * y_bar[j] * S.T)
        #
        # compute cost of VMs
        for j in range(S.cloud_start_index, S.FaaS_start_index):
            costs.append(S.resources[j].cost * y_bar[j] * S.T)
        #
        # compute the cost of FaaS and transition cost if not using SCAR
        if S.FaaS_start_index < J:
            key_list_comp = list(S.dic_map_com_idx.keys())
            val_list_comp = list(S.dic_map_com_idx.values())
            key_list_res = list(S.dic_map_res_idx.keys())
            val_list_res = list(S.dic_map_res_idx.values())
            for j in range(S.FaaS_start_index, J):
                for i in range(len(self.Y_hat)):
                    #part_indexes = np.nonzero(S.compatibility_matrix[i][:,j])[0]
                    part_indexes = np.nonzero(self.Y_hat[i][:,j])[0]
                    # get the name of component by its index
                    comp=key_list_comp[val_list_comp.index(i)]
                    for part_idx in part_indexes:
                        # get the name of partition by the index of the partition and its related component 
                        key_list_part = list(S.dic_map_part_idx[comp].keys())
                        val_list_part = list(S.dic_map_part_idx[comp].values())
                        part=key_list_part[val_list_part.index((i,part_idx))]
                        # get the name of resource by its index
                        res=key_list_res[val_list_res.index(j)]
                        # compute the cost of the FaaS
                        costs.append(S.resources[j].cost * \
                                     self.Y_hat[i][part_idx][j] * \
                                     S.faas_service_times[comp][part][res][0] * \
                                     S.components[i].comp_Lambda * \
                                     S.T)
        
        return sum(costs)
    
    
    ## Method to evaluate all performances and constraints
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return total cost
    def evaluation(self, S):
        self.cost = self.objective_function(S)
        all_performances=self.all_response_times(S)
        all_constraint_evaluation=self.all_constraints_evaluation(S)
        feasible=True
        edge=EdgePE()
        cloud=ServerFarmPE()
        utilizations=[]

        for j in range(S.cloud_start_index):
            utilization=edge.compute_utilization(j, self.Y_hat, S)
            if not math.isnan(utilization) and utilization>0:
                utilizations.append((j,utilization))
                if utilization>=1:
                    feasible=False
        

        for j in range(S.cloud_start_index,S.FaaS_start_index):
            utilization=cloud.compute_utilization(j, self.Y_hat, S)
            if not math.isnan(utilization) and utilization>0:
                utilizations.append((j,utilization))
                if utilization>=1:
                    feasible=False
        
        
        feasible= all([x[1] for x in all_constraint_evaluation[0] ] ) and all([x[1] for x in all_constraint_evaluation[1]])   
        return feasible,all_performances, all_constraint_evaluation, utilizations
    ## Method to convert the solution description into a json object
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param response_times Response times of all Graph.Component objects
    #                         (default: None)
    #   @param path_response_times Response times of all paths involved in 
    #                              Constraints.GlobalConstraint objects
    #                              (default: None)
    #   @param cost Cost of the Solution (default: None)
    #   @return Json object storing the solution description
    def to_json(self, S,feasible, response_times = None, path_response_times = None, cost = None):
      
        total_evaluation=self.evaluation(S)
        # compute response times of all components
       
        if not response_times:
            PE = SystemPerformanceEvaluator(self.logger)
            response_times = PE.compute_performance(S, self.Y_hat)
        
        solution_string = '{"Lambda": ' + str(S.Lambda)
       
        # write components deployments and response times
        solution_string += ',  "components": {'
        I = len(self.Y_hat)
        for i in range(I):
            component = S.components[i].name
            component_string = ' "' + component + '": {'
            allocation = np.nonzero(self.Y_hat[i])
            first_h= allocation[0][0]
            # get deployment name
            for dep in S.components[i].deployments:
               if first_h in dep.partitions_indices:
                   dep_name=dep.name
                   break
            component_string += ' "' + dep_name + '": {'
                
            # loop over partitions
            for idx in range(len(allocation[0])):
                
                # get partition and resource indices
                h = allocation[0][idx]
                j = allocation[1][idx]
               
                # get partition name
                partition = [key for key, (value1, value2) in \
                             S.dic_map_part_idx[component].items() \
                                 if value1 == i and value2 == h][0]
                component_string += ' "' + partition + '": {'
                # get computational layer name
                CL = S.resources[j].CLname
                component_string += '"' + CL + '": {'
                # get resource name and description
                resource = S.resources[j].name
                description = S.description[resource]
                if description == None:
                    description = "null"
                component_string += ('"' + resource + \
                                     '": {"description": "' + \
                                    description + '"')
                # get cost and memory
                res_cost = S.resources[j].cost * self.Y_hat[i][h,j]
                memory = S.resources[j].memory
                component_string += (', "cost": ' + str(res_cost) + \
                                     ', "memory": ' + str(memory))
                # get number of FaaS-related information
                if j < S.FaaS_start_index:  
                    number = int(self.Y_hat[i][h,j])
                    component_string += ', "number": ' + str(number) + '}},'
                else:
                    idle_time_before_kill = S.resources[j].idle_time_before_kill
                    transition_cost = S.resources[j].transition_cost
                    component_string += (', "idle_time_before_kill": ' + \
                                        str(idle_time_before_kill) + \
                                        ', "transition_cost": ' + \
                                        str(transition_cost) + '}},')
                
                # get the response time of partitions
                R=[item for item in total_evaluation[1] if item[0] == i and item[1] == h ]
                if R[0][2]<0:
                    component_string += ' "response_time": "inf" },'
                else:
                    component_string += ' "response_time": ' + str(R[0][2])+ '},'
           
            component_string = component_string[:-1] + '},'
            # get response time and corresponding threshold
            if response_times[i] == np.infty or response_times[i]<0:
                component_string += ' "response_time": "inf"'
            else:
                component_string += ' "response_time": ' + str(response_times[i])
            component_string += ', "response_time_threshold": '
            threshold = [LC.max_res_time for LC in S.local_constraints \
                         if LC.component_idx == i]
            if len(threshold) > 0:
                component_string += str(threshold[0]) + '},'
            else:
                component_string += '"inf"},'
            solution_string += component_string

        # write global constraints
        
        solution_string = solution_string[:-1] + '},  "global_constraints": {'
        for GCidx in range(len(S.global_constraints)):
            solution_string += S.global_constraints[GCidx].__str__(S.components)
            # write response time of the path
            solution_string = solution_string[:-1] + ', "path_response_time": '
            if path_response_times:
                if path_response_times[GCidx][1]== np.infty:
                    solution_string += ' "inf"'
                else:
                    solution_string += str(path_response_times[GCidx][1])
            else:
                path_name, f, time = total_evaluation[2][1][GCidx]
                if time== np.infty:
                    solution_string += ' "inf"'
                else:
                   
                    solution_string += str(time)
            solution_string += '},'

        if len(S.global_constraints) > 0:
            solution_string = solution_string[:-1]
        
        # write total cost
        solution_string += '},  "total_cost": "'
        if cost:
            solution_string += (str(cost) + '"')
        else:
            solution_string += (str(self.objective_function(S)) + '"')
        if feasible:
            solution_string +=', "feasible": true}'
        else:
            solution_string +=', "feasible": false}'
        
        # load string as json
        solution_string = solution_string.replace('0.,', '0.0,')
        jj = json.dumps(json.loads(solution_string), indent = 2)
        
       
        
        return total_evaluation, jj
    
    def to_json_unfeasible(self, S,total_evaluation, response_times = None, path_response_times = None, 
                 cost = None):

        if not response_times:
            PE = SystemPerformanceEvaluator(self.logger)
            response_times = PE.compute_performance(S, self.Y_hat)
        
        solution_string = '{"Lambda": ' + str(S.Lambda)

        # write components deployments and response times
        solution_string += ',  "components": { '
        I = len(self.Y_hat)
        # get the list of components included in local constraint
        LCcomponent_idx=[(C[0],C[1]) for C in total_evaluation[2][0]]

        for i in range(I):
            component = S.components[i].name
            R_cons=[a_tuple[1] for a_tuple in LCcomponent_idx if a_tuple[0]==i]
            if (len(R_cons)>0 and not R_cons[0]) or total_evaluation[1][i][2] == np.infty:
                component_string = ' "' + component + '": {'
                allocation = np.nonzero(self.Y_hat[i])
                first_h= allocation[0][0]
                # get deployment name
                for dep in S.components[i].deployments:
                   if first_h in dep.partitions_indices:
                       dep_name=dep.name
                       break
                component_string += ' "' + dep_name + '": {'
                # loop over partitions
                for idx in range(len(allocation[0])):
                    # get partition and resource indices
                    h = allocation[0][idx]
                    j = allocation[1][idx]
                    # get partition name
                    partition = [key for key, (value1, value2) in \
                                 S.dic_map_part_idx[component].items() \
                                     if value1 == i and value2 == h][0]
                   
                    component_string += ' "' + partition + '": {'
                    # get computational layer name
                    CL = S.resources[j].CLname
                    component_string += '"' + CL + '": {'
                    # get resource name and description
                    resource = S.resources[j].name
                    description = S.description[resource]
                    if description == None:
                        description = "null"
                    component_string += ('"' + resource + \
                                         '": {"description": "' + \
                                        description + '"')
                    # get cost and memory
                    res_cost = S.resources[j].cost * self.Y_hat[i][h,j]
                    memory = S.resources[j].memory
                    component_string += (', "cost": ' + str(res_cost) + \
                                         ', "memory": ' + str(memory))
                    # get number of FaaS-related information
                    if j < S.FaaS_start_index:  
                        number = int(self.Y_hat[i][h,j])
                        component_string += ', "number": ' + str(number) + '}},'
                    else:
                        idle_time_before_kill = S.resources[j].idle_time_before_kill
                        transition_cost = S.resources[j].transition_cost
                        component_string += (', "idle_time_before_kill": ' + \
                                            str(idle_time_before_kill) + \
                                            ', "transition_cost": ' + \
                                            str(transition_cost) + '}},')
                    
                    # get the response time of partitions
                    R=[item for item in total_evaluation[1] if item[0] == i and item[1] == h ]
                    if R[0][2]<0:
                        component_string += ' "response_time": "inf" },'
                    else:
                        component_string += ' "response_time": ' + str(R[0][2])+ '},'
           
          
                component_string = component_string[:-1] + '},'    
                # get response time and corresponding threshold
                if response_times[i] == np.infty or response_times[i]<0:
                    component_string += ' "response_time": "inf"'
                else:
                    component_string += ' "response_time": ' + str(response_times[i])
                component_string += ', "response_time_threshold": '
                threshold = [LC.max_res_time for LC in S.local_constraints \
                             if LC.component_idx == i]
                if len(threshold) > 0:
                    component_string += str(threshold[0]) + '},'
                else:
                    component_string += '"inf"},'
                solution_string += component_string
        
        # write global constraints
        
        solution_string = solution_string[:-1] + '},'
        if len(total_evaluation[2][1])>0:
            feasible=[x[1] for x in total_evaluation[2][1]]
            if not all(feasible):
                solution_string += ' "global_constraints": {'
                violated_idx= [i for i, x in enumerate(feasible) if not x]
                for GCidx in range(len(S.global_constraints)):
                    solution_string += S.global_constraints[GCidx].__str__(S.components)
                    # write response time of the path


                    path_name, f, time = total_evaluation[2][1][GCidx]
                    solution_string = solution_string[:-1] + ', "path_response_time": '
                    if time== np.infty:
                        solution_string += ' "inf"'
                    else:
                        solution_string += str(time)

                    solution_string += '},'
                solution_string = solution_string[:-1] + '},'
        if len(total_evaluation[3])>0:
            solution_string += ' "Resources": {'
            y_bar=self.get_y_bar()

            for j in range(len(total_evaluation[3])):
                res_idx=total_evaluation[3][j][0]
                utilization=total_evaluation[3][j][1]

                resource = S.resources[res_idx].name
                number = y_bar[res_idx]
                description = S.description[resource]
                solution_string += ('"' + resource + \
                                         '": {"description": "' + \
                                        description + '", "number": ' + str(number) + ', "utilization": ')
                solution_string +=str(utilization) + '},'
                

            solution_string = solution_string[:-1] + '} '
        solution_string = solution_string[:-1]+ '} '
        # load string as json
        solution_string = solution_string.replace('0.,', '0.0,')
        jj = json.dumps(json.loads(solution_string), indent = 2)
        return jj
       
        
    
    ## Method to print the solution in json format, either on screen or on 
    # the file whose name is passed as parameter
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param response_times Response times of all Graph.Component objects
    #                         (default: None)
    #   @param path_response_times Response times of all paths involved in 
    #                              Constraints.GlobalConstraint objects
    #                              (default: None)
    #   @param cost Cost of the Solution (default: None)
    #   @param solution_file Name of the file where the solution should be 
    #                        printed (default: "")
    def print_solution(self, S, response_times = None, 
                       path_response_times = None, 
                       cost = None, solution_file = "", feasible=True):

        # get solution description in json format
        total_evaluation, jj = self.to_json(S,feasible, response_times, path_response_times, cost)

        if solution_file:
            with open(solution_file, "w") as f:
                f.write(jj)
        else:
            print(jj)
            
        if not feasible:
         
            path=pathlib.Path(solution_file).parent.resolve()
            infeasible_file=str(path) + "/" + str(S.Lambda)+ "_infeasible.json"
            infeasible_json=self.to_json_unfeasible(S, total_evaluation, response_times, path_response_times, cost)
            with open(infeasible_file, "w") as f:
                f.write(infeasible_json)
        # print



## Result
class Result:
    
    ## @var ID
    # Unique id characterizing the Solution.Result (used for comparisons)
    
    ## @var solution
    # Candidate Solution.Configuration
    
    ## @var cost
    # Cost of the candidate Solution.Configuration
    
    ## @var performance
    # List whose first element is True if the Solution.Configuration is 
    # feasible, while the second and the third element store the paths and 
    # the components performance, respectively
    
    ## Result class constructor
    #   @param self The object pointer
    def __init__(self, log=space4ai_logger.Logger(name="SPACE4AI-D-Result")):
        self.ID = datetime.now().strftime("%Y%m%d-%H%M%S_") + str(uuid4())
        self.solution = None
        self.cost = np.infty
        self.performance = [False, None, None]
        self.violation_rate = np.infty
        self.logger = log
    
    ## Method to create a (cost, ID) pair to be used for comparison
    #   @param self The object pointer
    #   @return The (cost, ID) pair
    def _cmp_key(self):
        return (self.cost, self.ID)
    
    # Method to create a (-cost, ID) pair to be used for comparison
    #   @param self The object pointer
    #   @return The (-cost, ID) pair
    def _neg_cmp_key(self):
        return ( - self.cost, self.ID)
    
    ## Equality operator
    #   @param self The object pointer
    #   @param other The rhs of the comparison
    #   @return True if the two Configuration objects are equal
    def __eq__(self, other):
        return self._cmp_key() == other._cmp_key()
  
    ## Operator<
    #   @param self The object pointer
    #   @param other The rhs of the comparison
    #   @return True if lhs < rhs
    def __lt__(self, other):
        return self._cmp_key() < other._cmp_key()

    ## Method reduce the number of Resources.VirtualMachine objects in a
    # cluster
    #   @param self The object pointer
    #   @param resource_idx The index of the Resources.VirtualMachine object
    #   @param result The current Solution.Result object
    #   @return The updated Solution.Result object
    def reduce_cluster_size(self, resource_idx, system):

        # check if the resource index corresponds to an edge/cloud resource
        if resource_idx < system.FaaS_start_index:

            # check if more than one resource of the given type is available
            if system.resources[resource_idx].number > 1:

                # get the max number of used resources
                y_bar = self.solution.get_y_bar()

                # update the current solution, always checking its feasibility
                feasible = True
                while feasible and y_bar[resource_idx].max() > 1:

                    self.logger.log("y_bar[{}] = {}". \
                                    format(resource_idx, y_bar[resource_idx].max()), 7)

                    # create a copy of the current Y_hat matrix
                    temp = copy.deepcopy(self.solution.Y_hat)

                    # loop over all components
                    for i in range(len(self.solution.Y_hat)):
                        # loop over all component partitions
                        for h in range(len(self.solution.Y_hat[i])):
                            # decrease the number of resources (if > 1)
                            if temp[i][h, resource_idx] > 1:
                                temp[i][h, resource_idx] -= 1

                    # create a new solution with the updated Y_hat
                    new_solution = Configuration(temp)

                    # check if the new solution is feasible
                    new_performance = new_solution.check_feasibility(system)

                    # if so, update the result
                    feasible = new_performance[0]
                    if feasible:
                        # update the current solution
                        self.solution = new_solution
                        self.performance = new_performance
                        y_bar = self.solution.get_y_bar()
                        self.logger.log("feasible", 7)

    ## Method to check the feasibility of the current Configuration
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return performance
    def check_feasibility(self, S):
        self.performance = self.solution.check_feasibility(S)
        if not self.performance[0]:
            violation_ratio = 0
            if len(self.performance[1])>0:
                for path_idx in range(len(S.global_constraints)):
                    if not self.performance[1][path_idx][0] or self.performance[1][path_idx][1] is np.inf:
                        violation_ratio += (self.performance[1][path_idx][1] - S.global_constraints[path_idx].max_res_time)/S.global_constraints[path_idx].max_res_time

            for LC in S.local_constraints:
                if not self.performance[2][LC.component_idx][0] or self.performance[2][LC.component_idx][1] is np.inf:
                    violation_ratio += (self.performance[2][LC.component_idx][1] - LC.max_res_time)/LC.max_res_time
            if 0 < violation_ratio < np.inf:
                self.violation_rate = violation_ratio
        return self.performance
    
    ## Method to compute the cost of the current Configuration
    #   @param self The object pointer
    #   @param S A System.System object
    #   @return total cost
    def objective_function(self, S):
        self.cost = self.solution.objective_function(S)
        return self.cost

    
    ## Method to print the result in json format, either on screen or on 
    # the file whose name is passed as parameter
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param solution_file Name of the file where the solution should be 
    #                        printed (default: "")
    def print_result(self, S, solution_file = ""):
       # if self.performance[0]:
            feasible=self.performance[0]
            self.solution.print_solution(S, path_response_times=self.performance[1],
                                         cost=self.cost,
                                         solution_file=solution_file, feasible=feasible)
        # else:
        #     sfile = open(solution_file, "w") if solution_file else sys.stdout
        #     print("Unfeasible solution", file=sfile)
        #     if sfile != sys.stdout:
        #         sfile.close()

## EliteResults
# Class to store a fixed-size list of elite Solution.Result objects, sorted 
# by minimum cost
class EliteResults:
    
    ## @var elite_results
    # List of Solution.Result objects sorted by minimum cost
    
    ## @var K
    # Maximum length of the elite results list
    
    ## @var logger
    # Object of Logger type, used to print general messages
    
    ## EliteSolutions class constructor
    #   @param self The object pointer
    #   @param K Maximum length of the elite results list
    #   @param log Object of Logger type
    def __init__(
            self, K, 
            log=space4ai_logger.Logger(name="SPACE4AI-D-EliteResults")
        ):
        self.K = K
        self.elite_results = SortedList(key= attrgetter('cost','violation_rate'))#SortedList(key=lambda result: (result.cost, result.violation_rate))
        self.logger = log
        
    
    ## Method to add a Solution.Result object to the elite results list, 
    # keeping its length under control
    #   @param self The object pointer
    #   @param result Solution.Result object to be added to the list
    #   @param feasible_sol_found True if at least one feasible solution is found so far
    def add(self, result, feasible_sol_found = True):
        
        # check if the new result improves any elite result
        #if len(self.elite_results) == 0 or result.cost < self.elite_results[-1].cost:
        already_exist = False

        for res in self.elite_results:
            if (res.solution is not None) and (result.solution is not None) :
                if result.solution == res.solution:
                    already_exist = True
        if feasible_sol_found:
            if not already_exist and result.cost < self.elite_results[-1].cost:
                # add the new result to the list
                self.elite_results.add(result)

                # check if the total length exceeds than the maximum; if so,
                # remove the last element
                if len(self.elite_results) > self.K:
                    self.elite_results.pop()

                self.logger.log("Result improved - range: [{},{}]".\
                                format(self.elite_results[0].cost,
                                       self.elite_results[-1].cost), 2)
        else:
            if not already_exist and result.violation_rate < self.elite_results[-1].violation_rate:
            # add the new result to the list
                self.elite_results.add(result)

                # check if the total length exceeds than the maximum; if so,
                # remove the last element
                if len(self.elite_results) > self.K:
                    self.elite_results.pop()

                self.logger.log("Unfeasible result improved - range: [{},{}]".\
                                format(self.elite_results[0].violation_rate,
                                       self.elite_results[-1].violation_rate), 2)
    
    
    ## Method to merge two lists of elite results (inplace)
    #   @param self The object pointer
    #   @param other The EliteResults object to be merged (it remains 
    #                unchanged)
    def merge(self, other, feasible):
        
        # add all elements from the other list
        #self.elite_results.update(other.elite_results)

        # remove elements to keep the correct number of solutions
        #while len(self.elite_results) > self.K:
           # self.elite_results.pop()
        for result in other.elite_results:
            self.add(result, feasible)