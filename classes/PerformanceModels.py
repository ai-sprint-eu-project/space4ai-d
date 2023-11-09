from abc import ABC, abstractmethod


## BasePerformanceModel
#
# Abstract class used to represent a performance model for predicting the 
# response time of a Graph.Component.Partition object deployed onto a given 
# Resources.Resource
class BasePerformanceModel(ABC):
    
    ## @var keyword
    # Keyword identifying the model
    
    ## @var allows_colocation
    # True if Graph.Component.Partition objects relying on this model 
    # can be co-located on a device
    
    ## BasePerformanceModel class constructor
    #   @param self The object pointer
    #   @param keyword Keyword identifying the model
    #   @param **kwargs Additional (unused) keyword arguments
    def __init__(self, keyword, **kwargs):
        self.keyword = keyword
        self.allows_colocation = False
    
    ## Method to get a dictionary with the features required by the predict 
    # method
    #   @param self The object pointer
    #   @param **attributes Attributes that are used to retrieve the features
    #   @return The dictionary of the required features
    @abstractmethod
    def get_features(self, **attributes):
        pass
        
    ## Method to evaluate the object performance through the class predictor
    #   @param self The object pointer
    #   @param **features Model features
    #   @return Predicted response time
    @abstractmethod
    def predict(self, **features):
        pass
    
    ## Operator to convert a BasePerformanceModel object into a string
    #   @param self The object pointer
    def __str__(self):
        s = '"model":"{}"'.\
            format(self.keyword)
        return s

