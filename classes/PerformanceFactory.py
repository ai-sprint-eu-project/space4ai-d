from classes.PerformancePredictors import FaaSPredictorPacsltk
from classes.PerformancePredictors import FaaSPredictorMLlib
from classes.PerformancePredictors import CoreBasedPredictor
from classes.PerformancePredictors import LambdaBasedPredictor
from classes.PerformanceEvaluators import NetworkPerformanceEvaluator
from classes.PerformanceEvaluators import ServerFarmPE, EdgePE


## PerformanceFactory
#
# Class to build a factory of performance models/evaluators, characterized by 
# a given key and used to predict the response time of 
# Graph.Component.Partition objects executed on different Resources.Resource 
class PerformanceFactory: 

    ## @var models
    # Dictionary of available predictors
    
    ## PerformanceFactory class constructor
    def __init__(self):
        self.models = {}

    ## Method to register a new model/evaluator in the dictionary of available 
    # predictors
    #   @param self The object pointer
    #   @param key The key used to identify the performance model
    #   @param model The performance model
    def register(self, key, model):
        self.models[key] = model

    ## Method to initialize a new model/evaluator from the factory
    #   @param self The object pointer
    #   @param key The key used to identify the performance model
    #   @param **kwargs List of all parameter_name=parameter_value pairs 
    #                   that are required to initialize the model
    #   @return The performance model
    def create(self, key, **kwargs):
        model = self.models.get(key)
        if not model:
            raise ValueError(key)
        return model(**kwargs)


## Factory initialization
Pfactory = PerformanceFactory()
Pfactory.register("PACSLTK", FaaSPredictorPacsltk)
Pfactory.register("MLLIBfaas", FaaSPredictorMLlib)
Pfactory.register("QTcloud", ServerFarmPE)
Pfactory.register("QTedge", EdgePE)
Pfactory.register("CoreBasedPredictor", CoreBasedPredictor)
Pfactory.register("LambdaBasedPredictor", LambdaBasedPredictor)
Pfactory.register("NETWORK", NetworkPerformanceEvaluator)

