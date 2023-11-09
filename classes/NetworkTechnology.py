## NetworkDomain
#
# Class used to represent a network domain used for data transfer operations 
# among different Graph.Component or Graph.Component.Partition objects
class NetworkDomain:

    ## @var ND_name
    # Name of the network domain
    
    ## @var computationallayers
    # List of the names of the Resources.ComputationalLayer objects located 
    # in the network domain
    
    ## @var access_delay
    # Access delay characterizing the network domain

    ## @var bandwidth
    # Bandwidth characterizing the network domain
    
    ## @var performance_evaluator
    # Object of class PerformanceEvaluators.NetworkPerformanceEvaluator
    
    ## NetworkTechnology class constructor
    #   @param self The object pointer
    #   @param ND_name Name of the network domain
    #   @param computationallayers List of the names of the 
    #                              Resources.ComputationalLayer objects 
    #                              located in the network domain
    #   @param access_delay Access delay characterizing the network domain
    #   @param bandwidth Bandwidth characterizing the network domain
    #   @param performance_evaluator Object of class 
    #          PerformanceEvaluators.NetworkPerformanceEvaluator
    def __init__(self, ND_name, computationallayers, access_delay, 
                 bandwidth, performance_evaluator):
        self.ND_name = ND_name
        self.computationallayers = computationallayers
        self.access_delay = access_delay
        self.bandwidth = bandwidth
        self.performance_evaluator = performance_evaluator
    
    ## Method to evaluate the performance of the network domain, computing 
    # the time required to transfer a given amount of data
    #   @param data_size Amount of transferred data
    #   @return Network transfer time    
    def evaluate_performance(self, data_size):
        return self.performance_evaluator.predict(access_delay=self.access_delay,
                                                  bandwidth=self.bandwidth,
                                                  data=data_size)
        
    ## Operator to convert a NetworkTechnology object into a string
    #   @param self The object pointer
    def __str__(self):
        s = '"{}": {{"computationallayers":{}, "AccessDelay":{}, "Bandwidth":{}}}'.\
            format(self.ND_name, 
                   str(self.computationallayers).replace("'", '"'), 
                   self.access_delay, 
                   self.bandwidth)
        return s

