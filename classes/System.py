import pdb

from classes.Logger import Logger
from classes.Graph import DAG, Component
from classes.Resources import ComputationalLayer, VirtualMachine, EdgeNode, FaaS
from classes.NetworkTechnology import NetworkDomain
from classes.PerformanceFactory import Pfactory
from classes.PerformanceEvaluators import NetworkPerformanceEvaluator
from classes.Constraints import LocalConstraint, GlobalConstraint
import json
import sys
import numpy as np
import copy
import collections
from queue import Queue
from sortedcollections import OrderedSet

def recursivedict():
    return collections.defaultdict(recursivedict)

## System
#
class OrderedSetQueue(Queue):
    def _init(self, maxsize):
        self.queue = OrderedSet()
    def _put(self, item):
        self.queue.add(item)
    def _get(self):
        return self.queue.pop()
# Class to store the system description with all the relevant information
class System:
    
    ## @var cloud_start_index
    # Index of the first Resources.VirtualMachine object available in 
    # System.resources

    ## @var CLs
    # List of all the available Resources.ComputationalLayer objects
    
    ## @var compatibility_dict
    # Dictionary representing the compatibility of each 
    # Graph.Component.Partition object in a given Graph.Component 
    # with the available Resources.Resource objects
    
    ## @var compatibility_matrix 
    # List of 2D numpy arrays representing the compatibility between all resource
    # Graph.Component.Partition objects in each Graph.Component 
    # and the available Resources.Resource
    
    ## @var components 
    # List of all Graph.Component objects
            
    ## @var demand_matrix 
    # List of 2D numpy arrays representing the demand to run all 
    # Graph.Component.Partition objects in each Graph.Component 
    # on the available Resources.Resource
    
    ## @var description
    # Dictionary associating to each Resources.Resource object the 
    # corresponding description
    
    ## @var dic_map_com_idx
    # Dictionary associating to the name of each Graph.Component object the 
    # corresponding index in System.components
    
    ## @var dic_map_part_idx
    # Nested dictionary associating to the name of each Graph.Component and 
    # each Graph.Component.Partition in the Graph.Component a 
    # tuple whose first element is the index of the Graph.Component object 
    # in System.components and whose second element is the index of 
    # the Graph.Component.Partition
    
    ## @var dic_map_res_idx
    # Dictionary associating to the name of each Resources.Resource object 
    # the corresponding index in System.resources
    
    ## @var error
    # Object of Logger.Logger class, used to print error messages on sys.stderr
    
    ## @var faas_service_times
    # Dictionary storing the warm and cold service time for all 
    # Graph.Component.Partition objects executed on Resources.FaaS
    
    ## @var FaaS_start_index
    # Index of the first Resources.FaaS object available in System.resources
    
    ## @var global_constraints
    # List of Constraints.GlobalConstraint objects
    
    ## @var graph 
    # Object of Graph.DAG type
    
    ## @var local_constraints
    # List of Constraints.LocalConstraint objects
    
    ## @var Lambda
    # Incoming load
    
    ## @var logger
    # Object of Logger.Logger type, used to print general messages
    
    ## @var network_technologies
    # List of NetworkTechnology objects, characterized by a given access 
    # delay and bandwidth
    
    ## @var performance_models
    # List of 2D lists storing the performance model/evaluator initialized 
    # from the PerformanceFactory for each pair of Graph.Component.Partition 
    # and Resources.Resource object
    
    ## @var resources 
    # List of all the available Resources.Resource objects
    
    ## @var sorted_FaaS_by_memory_cost
    # List of Resources.FaaS objects sorted by memory (and then cost)
    
    ## @var sorted_FaaS_by_cost_memory
    # List of Resources.FaaS objects sorted by cost (and then memory)
    
    ## @var T
    # Time
  
    
    ## System class constructor: initializes all the System class members 
    # starting either from the configuration file or the json object passed 
    # as parameters
    #   @param self The object pointer
    #   @param system_file Configuration file describing the system
    #   @param system_json Json object describing the system
    #   @param log Object of Logger.Logger type
    def __init__(self, system_file="", system_json=None, Lambda=None, log=Logger()):
        self.logger = log
        self.error = Logger(stream=sys.stderr, verbose=1, error=True)
        self.Lambda = Lambda
        if system_file != "":
            self.logger.log("Loading system from configuration file", 1)
            self.read_configuration_file(system_file)
        elif system_json:
            self.logger.log("Loading system from json object", 1)
            self.load_json(system_json)
        else:
            self.error.log("No configuration file or json specified")
            sys.exit(1)
    
    
    ## Method to read a configuration file providing the system description 
    # (in json format) and populate the class members accordingly
    #   @param self The object pointer
    #   @param system_file Configuration file describing the system 
    #                      (json format)
    def read_configuration_file(self, system_file):
        
        # load json file
        with open(system_file) as f:
            data = json.load(f)
        
        self.load_json(data)
        
    
    ## Method to load the system description provided in json format and 
    # initialize all the class members accordingly
    #   @param self The object pointer
    #   @param data Json object describing the system 
    def load_json(self, data):
        
        # increase indentation level for logging
        self.logger.level += 1
        #pdb.set_trace()
        # initialize system DAG
        if "DirectedAcyclicGraph" in data.keys():
            self.logger.log("Initializing DAG", 2)
            DAG_dict = data["DirectedAcyclicGraph"]
            self.graph = DAG(graph_dict=DAG_dict,
                             log=Logger(stream=self.logger.stream,
                                        verbose=self.logger.verbose,
                                        level=self.logger.level+1))
        else:
            self.error.log("No DAG available in configuration file")
            sys.exit(1)
        
        # initialize lambda
        if self.Lambda is None:
            if "Lambda" in data.keys():
                self.logger.log("Initializing Lambda", 2)
                self.Lambda = float(data["Lambda"])
            else:
                self.error.log("No Lambda available in configuration file")
                sys.exit(1)
               
        # initialize components and local constraints
        if "Components" in data.keys():
            # load components info
            C = data["Components"]
            LC = None
            # load local constraints info
            if "LocalConstraints" in data.keys():
                self.logger.log("Initializing components and local constraints", 
                                2)
                LC = data["LocalConstraints"]
            else:
                self.logger.log("No local constraints specified", 3)
            # perform initialization
            self.initialize_components(C, LC)
        else:
            self.error.log("No components available in configuration file")
            sys.exit(1)
        
        # get global constraints to initialize the maximun response time of 
        # application paths
        if "GlobalConstraints" in data.keys():
            self.logger.log("Initializing global constraints", 2)
            GC = data["GlobalConstraints"]
            self.convert_GCdic_to_list(GC)
        else:
            self.global_constraints = []
            self.logger.log("No global constraints specified", 3)
        
        # initialize resources, together with their description and the 
        # dictionary that maps their names to their indices, and 
        # computational layers
        self.logger.log("Initializing resources and computational layers", 2)
        self.initialize_resources(data)
        
        # load Network Technology
        if "NetworkTechnology" in data.keys():
            self.logger.log("Initializing network technology", 2)
            self.network_technologies = []
            NT = data["NetworkTechnology"]
            # load Network Domains
            for ND in NT:
                if "computationalLayers" in NT[ND].keys() \
                    and "AccessDelay" in NT[ND].keys() \
                        and "Bandwidth" in NT[ND].keys():
                    network_domain = NetworkDomain(ND,
                                          list(NT[ND]["computationalLayers"]),
                                          float(NT[ND]["AccessDelay"]),
                                          float(NT[ND]["Bandwidth"]),
                                          NetworkPerformanceEvaluator())
                    self.network_technologies.append(network_domain)
                else:
                    self.error.log("Missing field in {} description".format(ND))
                    sys.exit(1)
        else:
            self.error.log("No NetworkTechnology available in configuration file", 1)
            sys.exit(1)
       
        # load dictionary of component-to-node compatibility 
        self.logger.log("Initializing compatibility matrix and performance-related components", 2)
        if "CompatibilityMatrix" in data.keys():
            self.compatibility_dict = data["CompatibilityMatrix"]
        else:
            self.error.log("No CompatibilityMatrix available in configuration file")
            sys.exit(1)
        
        # variable to check, for each component, if all resources mentioned 
        # in the demand matrix are compatible with the component itself
        is_compatible = True
     
        # load demand matrix
        if "Performance" in data.keys():
            performance_dict = data["Performance"]
            # check if, for each component, all resources mentioned in the 
            # performance dictionary are compatible with the component itself
            for c in performance_dict:
                for p in performance_dict[c]:
                    for r in performance_dict[c][p].keys():
                        compatible_res_list=[res["resource"] for res in self.compatibility_dict[c][p]]
                        if not r in compatible_res_list:
                            is_compatible = False
                            self.error.log("Performance dictionary and compatibility matrix are not consistent")
                            sys.exit(1)
        else:
            is_compatible = False
            self.error.log("No Performance dictionary available in configuration file")
            sys.exit(1)
       
        # if performance dictionary is available and it is consistent with 
        # compatibility matrix, convert both these dictionaries to arrays
        if is_compatible:
            self.convert_dic_to_matrix(performance_dict)
        
        # sort FaaS to have a list of sorted FaaS that is needed by Algorithm
        self.sort_FaaS_nodes()
        
        # initialize time
        self.logger.log("Initializing time", 2)
        if "Time" in data.keys():
            self.T = float(data["Time"])
        
        # restore indentation level for logging
        self.logger.level -= 1
       
 
    ## Method to initialize the components based on the dictionary of 
    # component extracted from config file and graph, and to compute the input 
    # lambda (workload) of each component. The input graph can be a general 
    # case with some parallel branches.
    # @param self The object pointer
    # @param C Dictionary of components came from configuration file.
    # @param LC Dictionary of LocalConstraints came from configuration file.
    def initialize_components(self, C, LC):
        self.components = []
        self.dic_map_com_idx = {}
        self.dic_map_part_idx = {}
        localconstraints = {}
        self.local_constraints = []
        comp_idx = 0
        # loop over components
        first_node = [node for node, in_degree in self.graph.G.in_degree if in_degree == 0]
        if len(first_node) > 1:
            self.error.log("The application graph must not have more than one start node")
            sys.exit(1)
        elif len(first_node) == 0:
            self.error.log("The application graph must have one start node")
            sys.exit(1)

        if set(self.graph.G.nodes) != set(C.keys()):
            self.error.log("No match between components in DAG and system input file")
            sys.exit(1)
        # Define a queue of component to visit the nodes of application DAG
        q = OrderedSetQueue(maxsize=len(self.graph.G.nodes))
        q.put(first_node[0])
        while not q.empty():
            node = q.get()
            can_handel = True
            # If the nodes that come to current node are not visited yet, we cannot visit current node, put it on the queue
            for n, c, data in self.graph.G.in_edges(node, data=True):
                if n not in self.dic_map_com_idx.keys():
                    q.put(node)
                    can_handel = False
                    break
            if can_handel:
                self.handel_component(C, node, comp_idx)
                # All the nodes that the current node comes to them should be put on the queue if they are not already there
                for n, c, data in self.graph.G.out_edges(node, data=True):
                    if c not in self.dic_map_com_idx.keys():
                        q.put(c)
            # initialize local constraint
                if LC and node in LC:
                    self.local_constraints.append(LocalConstraint(self.dic_map_com_idx[node],
                                                                  float(LC[node]["local_res_time"])))
                    localconstraints[node] = self.local_constraints[-1]
                comp_idx += 1

    def handel_component(self, C, node, comp_idx):
        if len(C[node]) > 0:
            deployments = []
            partitions = []
            part_idx = 0
            temp = {}
        # check if the node c has any input edge
        if self.graph.G.in_edges(node):
            Sum = 0
            # The component is not verified yet
            component_verified = False
            # if the node c has some input edges, its Lambda is equal
            # to the sum of products of lambda and weight of its
            # input edges.
            for n, c, data in self.graph.G.in_edges(node, data=True):
                prob = float(data["transition_probability"])
                ll = self.components[self.dic_map_com_idx[n]].comp_Lambda
                Sum += prob * ll
                # loop over all candidate deployments
            for s in C[node]:
                part_Lambda = -1
                part_idx_list = []
                if len(C[node][s]) > 0:
                    # loop over all partitions
                    for h in C[node][s]:
                        if part_Lambda > -1:
                            prob = float(C[node][s][prev_part]["early_exit_probability"])
                            part_Lambda *= (1 - prob)
                        else:
                            part_Lambda = copy.deepcopy(Sum)
                        temp[h] = (comp_idx, part_idx)
                        partitions.append(Component.Partition(h, part_Lambda,
                                                              float(C[node][s][h]["early_exit_probability"]),
                                                              C[node][s][h]["next"],
                                                              C[node][s][h]["data_size"]))
                        part_idx_list.append(part_idx)
                        part_idx += 1
                        prev_part = h
                deployments.append(Component.Deployment(s, part_idx_list))
            self.dic_map_part_idx[node] = temp
            comp = Component(node, deployments, partitions, Sum)
            self.components.append(comp)
        else:
            # if the node c does not have any input edge, it is the
            # first node of a path and its Lambda is equal to input
            # lambda
            partitions = []
            for s in C[node]:
                part_Lambda = -1
                part_idx_list = []
                if len(C[node][s]) > 0:

                    # loop over all partitions
                    for h in C[node][s]:
                        if part_Lambda > -1:

                            prob = float(C[node][s][prev_part]["early_exit_probability"])
                            part_Lambda *= (1 - prob)
                        else:
                            part_Lambda = copy.deepcopy(self.Lambda)
                        temp[h] = (comp_idx, part_idx)
                        partitions.append(Component.Partition(h, part_Lambda,
                                                              float(C[node][s][h]["early_exit_probability"]),
                                                              C[node][s][h]["next"],
                                                              C[node][s][h]["data_size"]))
                        part_idx_list.append(part_idx)
                        part_idx += 1
                        prev_part = h
                deployments.append(Component.Deployment(s, part_idx_list))
            self.dic_map_part_idx[node] = temp
            self.components.append(Component(node, deployments, partitions, self.Lambda))

        self.dic_map_com_idx[node] = comp_idx
    ## Method to initialize resources, together with their description and the 
    # dictionary that maps their names to their indices, and 
    # computational layers
    #   @param self The object pointer
    #   @param data Json object storing all the relevant information
    def initialize_resources(self, data):
        
        # increase indentation level for logging
        self.logger.level += 1
        
        self.resources = []
        self.description = {}
        self.dic_map_res_idx = {}
        self.CLs = []
        resource_idx = 0
        #
        # edge resources
        if "EdgeResources" in data.keys():
            self.logger.log("Edge resources", 3)
            ER = data["EdgeResources"]
            # loop over computational layers
            for CL in ER:
                cl = ComputationalLayer(CL)
                # loop over nodes and add them to the corresponding layer
                for node in ER[CL]:
                    temp = ER[CL][node]
                    if "number" in temp.keys() and "cost" in temp.keys() \
                        and "memory" in temp.keys() \
                            and "n_cores" in temp.keys():
                        new_node = EdgeNode(CL, node, float(temp["cost"]),
                                            float(temp["memory"]),
                                            int(temp["number"]),
                                            int(temp["n_cores"]))
                        self.resources.append(new_node)
                        self.dic_map_res_idx[node] = resource_idx
                        cl.add_resource(resource_idx)
                        resource_idx += 1
                    else:
                        self.error.log("Missing field in {} description".\
                                       format(node))
                        sys.exit(1)
                    # add the resource description to the corresponding 
                    # dictionary
                    if "description" in temp.keys():
                        self.description[node] = temp["description"]
                    else:
                        self.description[node] = "No description"
                # add the new computational layer
                self.CLs.append(cl)    
        #
        # cloud resources
        self.cloud_start_index = resource_idx
        if "CloudResources" in data.keys():
            self.logger.log("Cloud resources", 3)
            CR = data["CloudResources"]
            # loop over computational layers
            for CL in CR:
                cl = ComputationalLayer(CL)
                # loop over VMs and add them to the corresponding layer
                for VM in CR[CL]:
                    temp = CR[CL][VM]
                    if "number" in temp.keys() and "cost" in temp.keys() \
                        and "memory" in temp.keys() \
                            and "n_cores" in temp.keys():
                        new_vm = VirtualMachine(CL, VM, float(temp["cost"]), 
                                                float(temp["memory"]), 
                                                int(temp["number"]),
                                                int(temp["n_cores"]))
                        self.resources.append(new_vm)
                        self.dic_map_res_idx[VM] = resource_idx
                        cl.add_resource(resource_idx)
                        resource_idx += 1
                    else:
                        self.error.log("Missing field in {} description".\
                                       format(VM))
                        sys.exit(1)
                    # add the resource description to the corresponding 
                    # dictionary
                    if "description" in temp.keys():
                        self.description[VM] = temp["description"]
                    else:
                        self.description[VM] = "No description"
                # add the new computational layer
                self.CLs.append(cl)
        #
        # faas resources
        self.FaaS_start_index = resource_idx
        if "FaaSResources" in data.keys():
            self.logger.log("FaaS resources", 3)
            FR = data["FaaSResources"]
            # loop over computational layers
            for CL in FR:
                if CL.lower().startswith("computationallayer"):
                    cl = ComputationalLayer(CL)
                    # initialize transition cost
                    if "transition_cost" in FR[CL].keys():
                        transition_cost = float(FR[CL]["transition_cost"])
                    else:
                        self.error.log("Missing transition cost in {}".\
                                       format(CL))
                        sys.exit(1)
                    # loop over functions and add them to the corresponding 
                    # layer
                    for func in FR[CL]:
                        if func != "transition_cost":
                            temp = FR[CL][func]
                            if "cost" in temp.keys() and "memory" in temp.keys() \
                                and "idle_time_before_kill" in temp.keys():
                                new_f = FaaS(CL, func, float(temp["cost"]), 
                                             float(temp["memory"]), 
                                             transition_cost, 
                                             float(temp["idle_time_before_kill"]))
                                self.resources.append(new_f)
                                self.dic_map_res_idx[func]=resource_idx
                                cl.add_resource(resource_idx)
                                resource_idx += 1
                            else:
                                self.logger.log("Missing field in {} description".\
                                                format(func), 1)
                                sys.exit(1)
                            # add the resource description to the corresponding 
                            # dictionary
                            if "description" in temp.keys():
                                self.description[func] = temp["description"]
                            else:
                                self.description[func] = "No description"
                    # add the new computational layer
                    self.CLs.append(cl)

        # restore indentation level for logging
        self.logger.level -= 1

    ## Method to convert the dictionary of global constraints to a list
    # @param self The object pointer   
    # @param GC Dictionary of global constraints
    def convert_GCdic_to_list(self, GC):
        self.global_constraints = []
        # loop over paths
        for p in GC:
            C_list = []
            # loop over components in the path
            for c in GC[p]["components"]:
                if c in self.dic_map_com_idx.keys():
                    C_list.append(list(self.dic_map_com_idx.keys()).index(c))
                else:
                    self.error.log("No match between components and path in global constraints")
                    sys.exit(1)
            self.global_constraints.append(GlobalConstraint(C_list, 
                                                            GC[p]["global_res_time"],
                                                            p))
    
    
    ## Method to convert the compatibility and performance dictionaries into 
    # two lists of 2D numpy arrays such that M[i][h,j] represents either the 
    # compatibility of partition h in component i with resource j or the 
    # demand to run such partition on the given resource, a list of 2D 
    # lists storing the performance models, and a dictionary storing warm 
    # and cold service times for all partitions executed on FaaS resources
    #    @param self The object pointer
    #    @param performance_dict Dictionary of performance-related information
    def convert_dic_to_matrix(self, performance_dict):
        self.compatibility_matrix = []
        self.compatibility_matrix_memory = []
        self.demand_matrix = []
        self.performance_models = []
        self.faas_service_times = recursivedict()
        # count the total number of resources
        r = len(self.resources)
        # loop over components
        for comp_idx, comp in enumerate(self.components):
            # count the total number of partitions
            p = len(comp.partitions)
            # define and initialize the matrices to zero
            self.compatibility_matrix.append(np.full((p, r), 0, dtype = int))
            self.compatibility_matrix_memory.append(np.full((p, r), 0, dtype = int))
            self.demand_matrix.append(np.full((p, r), 0, dtype=float))
            # define and initialize the performance models to None
            self.performance_models.append([[None] * r] * p)
            # loop over partitions
            for part_idx, part in enumerate(comp.partitions):
                # loop over resources
                compatible_res_list=[res["resource"] for res in self.compatibility_dict[comp.name][part.name]]
                for res in compatible_res_list:
                    res_idx = self.dic_map_res_idx[res]
                    # set to 1 the element in the compatibility matrix
                    self.compatibility_matrix[comp_idx][part_idx][res_idx] = 1
                    memory=next(item["memory"] for item in self.compatibility_dict[comp.name][part.name] if item["resource"] == res)
                    self.compatibility_matrix_memory[comp_idx][part_idx][res_idx] = memory
                    # set the performance model
                    perf_data = performance_dict[comp.name][part.name][res]
                    if "model" in perf_data.keys():
                        model_data = {}
                        for key in perf_data.keys():
                            if key != "model" and not key.startswith("demand"):
                                model_data[key] = perf_data[key]
                        m = Pfactory.create(perf_data["model"], **model_data)
                        self.performance_models[comp_idx][part_idx][res_idx] = m
                    else:
                        self.error.log("Missing performance model/evaluator")
                        sys.exit(1)
                    # get the demand (if available)
                    d = np.nan
                    # For Edge and Cloud resources, the demand is taken 
                    # directly from the dictionary
                    if res_idx < self.FaaS_start_index and \
                            "demand" in perf_data.keys():
                        d = performance_dict[comp.name][part.name][res]["demand"]
                    else:
                        # for FaaS resources, it should be computed accordingly
                        #
                        if "demandWarm" in perf_data.keys() and \
                                "demandCold" in perf_data.keys():
                            warm_service_time = perf_data["demandWarm"]
                            cold_service_time = perf_data["demandCold"]
                            # add the warm and cold service time to the 
                            # corresponding dictionary
                            self.faas_service_times[comp.name][part.name][res] = [warm_service_time,
                                                                                 cold_service_time]

                            # compute the demand
                            pm = self.performance_models[comp_idx][part_idx][res_idx]
                            features = pm.get_features(c_idx=comp_idx, 
                                                       p_idx=part_idx, 
                                                       r_idx=res_idx, 
                                                       S=self)
                            d = pm.predict(**features)


                    # write the demand into the matrix
                    self.demand_matrix[comp_idx][part_idx, res_idx] = d
    
    
    ## Method to sort all input FaaS nodes increasingly by memory 
    #   @param self The object pointer
    #   @return 1) The sorted list of resources by memory and cost, respectively. 
    #           Each item of list includes the index, memory and cost of the resource.
    #           The list is sorted by memory, but for the nodes with the same memory, it is sorted by cost
    #
    def sort_FaaS_nodes(self):
        idx_min_memory_node=[]
        for i, c in enumerate(self.components):
            # loop over partitions
            for h, part in enumerate(c.partitions):
                # loop over all FaaS
                compatible_res_list=[res["resource"] for res in self.compatibility_dict[c.name][part.name]]
                for res in compatible_res_list:
                    j = self.dic_map_res_idx[res]
                    if j >= self.FaaS_start_index:
                        # add the information of node to the list includes node index, memory and cost
                        # The cost that we consider is the product of time unit cost and  warm service time(d_hot)
                        idx_min_memory_node.append((j, self.resources[j].memory, self.resources[j].cost * self.faas_service_times[c.name][part.name][res][0]))
                
        # Sort the list based on memory and cost respectively    
        # Each item of list includes the index, memory and cost of the resource.
        # The list is sorted by memory, but for the nodes with the same memory, it is sorted by cost
        self.sorted_FaaS_by_memory_cost = sorted(idx_min_memory_node, key=lambda element: (element[1], element[2]))
        # Sort the list based on cost and memory respectively
        # Each item of list includes the index, utilization and cost of the resource.
        # The list is sorted by utilization, but for the nodes with same utilization, it is sorted by cost

        self.sorted_FaaS_by_cost_memory = sorted(idx_min_memory_node, key=lambda element: (element[2], element[1]))
       
    def read_solution_file(self,solution_file):
         with open(solution_file) as f:
            data = json.load(f)
         return data
    
    ## Method to convert the system description into a json object
    #   @param self The object pointer
    #   @return Json object storing the system description
    def to_json(self):
        
        # components
        system_string = '{"Components": {'
        for c in self.components:
            system_string += (str(c) + ',')
        system_string = system_string[:-1] + '}'
        
        # resources
        system_string += ', '
        edge_string = '"EdgeResources": {'
        cloud_string = '"CloudResources": {'
        faas_string = '"FaaSResources": {'
        for CL in self.CLs:
            last_idx = CL.resources[-1]
            if last_idx < self.cloud_start_index:
                edge_string += (CL.__str__(self.resources) + ',')
            elif last_idx < self.FaaS_start_index:
                cloud_string += (CL.__str__(self.resources) + ',')
            else:
                faas_string += (CL.__str__(self.resources) + ',')
        system_string += (edge_string[:-1] + '}, \n' + \
                          cloud_string[:-1] + '}, \n' + \
                          faas_string[:-1] + '}')
                
        # compatibility matrix
        system_string += (', \n"CompatibilityMatrix": ' + \
                          str(self.compatibility_dict).replace("\'", "\""))
        
        # demand matrix
        system_string += ', \n"Performance": {'
        for i, c in enumerate(self.components):
            c = self.components[i]
            component_string = '"' + c.name + '": {'
            for h, p in enumerate(c.partitions):
                component_string += ('"' + p.name + '": {')
                compatible_res_list=[res["resource"] for res in self.compatibility_dict[c.name][p.name]]
                for res in compatible_res_list:
                    component_string += ('"' + res + '": {')
                    j = self.dic_map_res_idx[res]
                    component_string += str(self.performance_models[i][h][j])
                    if not np.isnan(self.demand_matrix[i][h,j]):
                        component_string += (', "demand": ' + \
                                             str(self.demand_matrix[i][h,j]))
                    if j >= self.FaaS_start_index:
                        dw = self.faas_service_times[c.name][p.name][res][0]
                        dc = self.faas_service_times[c.name][p.name][res][1]
                        component_string += (', "demandWarm": ' + \
                                             str(dw) + ', "demandCold": ' +\
                                             str(dc))
                    component_string += '},'
                component_string = component_string[:-1] + '},'
            system_string += (component_string[:-1] + '},')
        system_string = system_string[:-1] + '}'
        
        # lambda
        system_string += (', \n"Lambda": ' + str(self.Lambda))
        
        # local constraints
        system_string += ', \n"LocalConstraints": {'
        for LC in self.local_constraints:
            system_string += (LC.__str__(self.components) + ',')
        system_string = system_string[:-1] + '}'
        
        # global constraints
        system_string += ', \n"GlobalConstraints": {'
        for GC in self.global_constraints:
            system_string += (GC.__str__(self.components) + ',')
        system_string = system_string[:-1] + '}'
        
        # network technology
        system_string += ', \n"NetworkTechnology": {'
        for d in self.network_technologies:
            system_string += (str(d) + ',')
        system_string = system_string[:-1] + '}'
        
        # DAG
        system_string += (', \n"DirectedAcyclicGraph": {' + \
                          str(self.graph) + '}')
                          
        system_string += (', \n"Time": ' + str(self.T) + '}')
        
        # load string as json
        jj = json.dumps(json.loads(system_string), indent = 2)
        
        return jj

        
    ## Method to print the system description (in json format), either on 
    # stdout or onto a given file
    #   @param self The object pointer
    #   @param system_file File where to print the system description (optional)
    def print_system(self, system_file = ""):
        
        # get system description in json format
        jj = self.to_json()
        
        # print
        if system_file:
            with open(system_file, "w") as f:
                f.write(jj)
        else:
            print(jj)


    ## Method to print the graph onto a gml file (see Graph.DAG.write_DAG)
    #   @param self The object pointer
    #   @param graph_file File where to print the graph (gml format)
    def print_graph(self, graph_file):
        self.graph.write_DAG(graph_file)


    ## Method to plot the graph (see Graph.DAG.plot_DAG)
    #   @param self The object pointer
    #   @param plot_file File where to plot the graph (optional)
    def plot_graph(self, plot_file = ""):
        self.graph.plot_DAG(plot_file)
