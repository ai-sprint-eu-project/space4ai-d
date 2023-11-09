from classes.Logger import Logger
from classes.PerformanceModels import BasePerformanceModel
from abc import abstractmethod
import numpy as np
import sys


## NetworkPerformanceEvaluator
#
# Class designed to evaluate the performance of a NetworkTechnology object, 
# namely the time required to transfer data between two consecutive 
# Graph.Component or Graph.Component.Partition objects executed on 
# different devices in the same network domain
class NetworkPerformanceEvaluator(BasePerformanceModel):
    
    ## FaaSPredictor class constructor
    #   @param self The object pointer
    #   @param **kwargs Additional (unused) keyword arguments
    def __init__(self, **kwargs):
        super().__init__("NETWORK")
        
    ## Method to get a dictionary with the features required by the predict 
    # method
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param c_idx Index of the Graph.Component object
    #   @param p_idx Index of the Graph.Component.Partition object
    #   @param S A System.System object
    #   @param ND A NetworkTechnology.NetworkDomain object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return The dictionary of the required features
    def get_features(self, *, c_idx, p_idx, S, ND, **kwargs):
        features = {"access_delay": ND.access_delay,
                    "bandwidth": ND.bandwidth,
                    "data": S.components[c_idx].partitions[p_idx].data_size}
        return features

    ## Method to evaluate the performance of a NetworkTechnology object
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param access_delay Access delay characterizing the network domain
    #   @param bandwidth Bandwidth characterizing the network domain
    #   @param data Amount of data transferred
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Network transfer time
    def predict(self, access_delay, bandwidth, data, **kwargs):
        # Convert MB to Mb by multiplying 8
        return access_delay + (data * 8 / bandwidth)


## QTPerformanceEvaluator
#
# Abstract class used to represent a performance evaluator, namely an object 
# that evaluates the performance of a Graph.Component.Partition executed on 
# different types of resources, exploiting the M/G/1 queue model
class QTPerformanceEvaluator(BasePerformanceModel):
    
    ## @var allows_colocation
    # True if Graph.Component.Partition objects relying on this model 
    # can be co-located on a device
    
    ## QTPerformanceEvaluator class constructor
    #   @param self The object pointer
    #   @param keyword Keyword identifying the evaluator
    #   @param **kwargs Additional (unused) keyword arguments
    def __init__(self, keyword, **kwargs):
        super(QTPerformanceEvaluator, self).__init__(keyword)
        self.allows_colocation = True
    
    ## Method to get a dictionary with the features required by the predict 
    # method
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param c_idx Index of the Graph.Component object
    #   @param p_idx Index of the Graph.Component.Partition object
    #   @param r_idx Index of the Resources.Resource object
    #   @param S A System.System object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return The dictionary of the required features
    def get_features(self, *, c_idx, p_idx, r_idx, S, Y_hat, **kwargs):
        features = {"i": c_idx,
                    "h": p_idx,
                    "j": r_idx,
                    "Y_hat": Y_hat,
                    "S": S}
        return features
    
    ## Method to compute the utilization of a specific 
    # Resources.Resource object given the Graph.Component.Partition objects 
    # executed on it
    #   @param self The object pointer
    #   @param j Index of the Resources.Resource object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Utilization of the given Resources.Resource object
    @abstractmethod
    def compute_utilization(self, j, Y_hat, S, **kwargs):
        pass

    ## Method to evaluate the performance of a specific 
    # Graph.Component.Partition object executed onto a specific 
    # Resources.Resource
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param i Index of the Graph.Component
    #   @param h Index of the Graph.Component.Partition
    #   @param j Index of the Resources.Resource
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Response time
    @abstractmethod
    def predict(self, *, i, h, j, Y_hat, S, **kwargs):
        pass
    

## ServerFarmPE
#
# Class designed to evaluate the performance of a Graph.Component.Partition 
# object executed in a server farm (i.e., a group of Resources.VirtualMachine 
# objects)
class ServerFarmPE(QTPerformanceEvaluator):
    
    ## ServerFarmPE class constructor
    #   @param self The object pointer
    #   @param **kwargs Additional (unused) keyword arguments
    def __init__(self, **kwargs):
        super(ServerFarmPE, self).__init__("QTcloud")
    
    ## Method to compute the utilization of a specific 
    # Resources.VirtualMachine object
    #   @param self The object pointer
    #   @param j Index of the Resources.VirtualMachine object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Utilization of the given Resources.VirtualMachine object
    def compute_utilization(self, j, Y_hat, S, **kwargs):
        utilization = 0
        # loop over all components
        for i, c in enumerate(S.components):
            # loop over all partitions in the component
            for h, p in enumerate(c.partitions):
                # compute the utilization
                if Y_hat[i][h,j] > 0:
                    utilization += S.demand_matrix[i][h,j] * \
                                    p.part_Lambda / Y_hat[i][h,j]
                                        
        return utilization
    
    ## Method to evaluate the performance of a specific 
    # Graph.Component.Partition object executed onto a 
    # Resources.VirtualMachine
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param i Index of the Graph.Component
    #   @param h Index of the Graph.Component.Partition
    #   @param j Index of the Resources.VirtualMachine
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Response time
    def predict(self, *, i, h, j, Y_hat, S, **kwargs):
        # compute the utilization
        utilization = self.compute_utilization(j, Y_hat, S)
        # compute the response time
        r = 0.
        if Y_hat[i][h,j] > 0:
            r = S.demand_matrix[i][h,j] / (1 - utilization) 
        return r


## EdgePE
#
# Class designed to evaluate the performance of a Graph.Component.Partition  
# object executed on a Resources.EdgeNode 
class EdgePE(QTPerformanceEvaluator):
    
    ## EdgePE class constructor
    #   @param self The object pointer
    #   @param **kwargs Additional (unused) keyword arguments
    def __init__(self, **kwargs):
        super(EdgePE, self).__init__("QTedge")
    
    ## Method to compute the utilization of a specific 
    # Resources.EdgeNode object
    #   @param self The object pointer
    #   @param j Index of the Resources.EdgeNode object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Utilization of the given Resources.EdgeNode object
    def compute_utilization(self, j, Y_hat, S, **kwargs):
        utilization = 0
        # loop over all components
        for i, c in enumerate(S.components):
            # loop over all partitions in the component
            for h, p in enumerate(c.partitions):
                # compute the utilization
                if Y_hat[i][h,j] > 0:
                    utilization += S.demand_matrix[i][h,j] * \
                                    p.part_Lambda / Y_hat[i][h,j]
                #utilization += S.demand_matrix[i][h,j] * \
                 #               Y_hat[i][h,j] * p.part_Lambda
        return utilization
    
    ## Method to evaluate the performance of a specific 
    # Graph.Component.Partition object executed onto a Resources.EdgeNode
    #   @param self The object pointer
    #   @param * Positional arguments are not accepted
    #   @param i Index of the Graph.Component
    #   @param h Index of the Graph.Component.Partition
    #   @param j Index of the Resources.EdgeNode
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param S A System.System object
    #   @param **kwargs Additional (unused) keyword arguments
    #   @return Response time
    def predict(self, *, i, h, j, Y_hat, S, **kwargs):
        # compute utilization
         utilization = self.compute_utilization(j, Y_hat, S)
        # compute response time
         r = 0.
         if Y_hat[i][h,j] > 0:
            r = S.demand_matrix[i][h,j] / (1 - utilization)
         return r
        #return S.demand_matrix[i][h,j] * Y_hat[i][h,j] / (1 - utilization)



## SystemPerformanceEvaluator
#
# Class used to evaluate the performance of a Graph.Component object given 
# the information about the Resources.Resource where it is executed
class SystemPerformanceEvaluator:
    
    ## @var logger
    # Object of Logger type, used to print general messages
    
    
    ## SystemPerformanceEvaluator class constructor
    #   @param self The object pointer
    #   @param log Object of Logger type
    def __init__(self, log=Logger()):
        self.logger = log
        self.error = Logger(stream=sys.stderr, verbose=1, error=True)
    
    
    ## Method to evaluate the response time of the Graph.Component object 
    # identified by the given index
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @param c_idx The index of the current component
    #   @return Response time
    def get_perf_evaluation(self, S, Y_hat, c_idx):
        
        # check if the memory constraints are satisfied
        self.logger.log("Evaluating component {}".format(c_idx), 5)
        
        # initialize response time
        perf_evaluation = 0
        
        # get the indices of the resource where the partitions of the current 
        # component are executed
        j = np.nonzero(Y_hat[c_idx])
        
        # loop over all partitions
        self.logger.level += 1

        prev_parts_idx = []
        for h in range(len(j[0])):
            # evaluate the response time
            p_idx = j[0][h]
            r_idx = j[1][h]
            self.logger.log("Evaluating partition response times", 6)
            self.logger.level += 1
            if r_idx < S.FaaS_start_index:
                PM = S.performance_models[c_idx][p_idx][r_idx]
                features = PM.get_features(c_idx=c_idx, p_idx=p_idx,
                                           r_idx=r_idx, S=S, Y_hat=Y_hat)
                p = PM.predict(**features)
                self.logger.log("features: {}".format(features), 7)
            else:
                p = S.demand_matrix[c_idx][p_idx,r_idx]
            self.logger.log("(h:{}, j:{}) --> {}".format(h, r_idx, p), 7)
            self.logger.level -= 1
            self.logger.log("time --> {}".format(p), 6)
            if len(prev_parts_idx) == 0:
                perf_evaluation += p
                prev_parts_idx.append(p_idx)
            else:
                early_exit_prob=1
                for part_idx in prev_parts_idx:
                    early_exit_prob *= (1-S.components[c_idx].partitions[part_idx].early_exit_probability)
                prev_parts_idx.append(p_idx)
                network_delay = 0
                # check if two partitions are in the same device
                self.logger.log("Evaluating network delay", 6)
                self.logger.level += 1
                if not j[1][h-1] == j[1][h]:
                    # get the data transferred from the partition
                    data_size = S.components[c_idx].partitions[j[0][h-1]].data_size[0]
                    # compute the network transfer time
                    network_delay = self.get_network_delay(j[1][h-1], j[1][h], S, data_size)
                    self.logger.log("{} --> {}".format(h, network_delay), 7)
                self.logger.level -= 1
                self.logger.log("time --> {}".format(network_delay), 6)
                perf_evaluation += early_exit_prob * (p + network_delay)
        self.logger.level -= 1
        self.logger.log("time --> {}".format(perf_evaluation), 5)

        
        return perf_evaluation

    
    ## Method to evaluate the response time of the all Graph.Component objects
    #   @param self The object pointer
    #   @param S A System.System object
    #   @param Y_hat Matrix denoting the amount of Resources assigned to each 
    #                Graph.Component.Partition object
    #   @return 1D numpy array with the response times of all components
    def compute_performance(self, S, Y_hat):
        I = len(Y_hat)
        response_times = np.full(I, np.inf)
        for i in range(I):
            response_times[i] = self.get_perf_evaluation(S, Y_hat, i)
        return response_times


    ## Static method to compute the network delay due to data transfer 
    # operations between two consecutive components (or partitions), executed 
    # on different resources in the same network domain
    #   @param self The object pointer
    #   @param cpm1_resource Resource index of first component
    #   @param cpm2_resource Resource index of second component
    #   @param S A System.System object
    #   @param data_size Amount of transferred data
    #   @return Network transfer time
    def get_network_delay(self, cpm1_resource, cpm2_resource, S, data_size):
       
        # get the names of the computational layers where the two resources 
        # are located
        CL1 = S.resources[cpm1_resource].CLname
        CL2 = S.resources[cpm2_resource].CLname
        
        # get the lists of network domains containing the two computational 
        # layers and compute their intersection
        ND1 = list(filter(lambda NT: (CL1 in NT.computationallayers), S.network_technologies))
        ND2 = list(filter(lambda NT: (CL2 in NT.computationallayers), S.network_technologies))
        ND = list(set(ND1).intersection(ND2))
      
        # there must exist a common network domain, otherwise the components
        # cannot communicate with each other
        if len(ND) == 0:
            self.error.log("ERROR: no network domain available between {} and {}".\
                           format(cpm1_resource, cpm2_resource))
            sys.exit(1)
        # if only one domain is common to the two layers, evaluate the 
        # network delay on that domain
        elif len(ND) == 1:
            network_delay = ND[0].evaluate_performance(data_size)
        else:
            # else, the network transfer time is the minimum among the times 
            # required with the different network domains
            network_delay = float("inf")
            for nd in ND:
                new_network_delay = nd.evaluate_performance(data_size)
                if new_network_delay < network_delay:
                   network_delay = new_network_delay 
        
        return network_delay

