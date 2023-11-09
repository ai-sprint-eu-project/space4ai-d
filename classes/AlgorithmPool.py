from classes.Algorithm import RandomGreedy
from classes.Heuristics import Tabu_Search, Local_Search, Simulated_Annealing, Genetic_Algorithm
from classes.BaseHeuristics import BinarySearch


## AlgorithmPool
#
# Class to build a pool of algorithms, specified by a given key for each algorithm
class AlgorithmPool:

    ## @var algorithms
    # Dictionary of available algorithms

    ## AlgorithmPool class constructor
    def __init__(self):
        self.algorithms = {}

    ## Method to submit a new algorithm in the dictionary of available
    # algorithms
    #   @param self The object pointer
    #   @param key The key used to identify the algorithm
    #   @param algorithm The algorithm name
    def submit(self, keys, algorithm):
        self.algorithms.update(dict.fromkeys(keys, algorithm))


    ## Initialize a new algorithm from the pool
    #   @param self The object pointer
    #   @param key The key used to identify the new algorithm
    #   @param **kwargs dictionary of all parameters required to initialize the algorithm
    #   @return The performance model
    def create(self, key, **kwargs):
        algorithm = self.algorithms.get(key)
        if not algorithm:
            raise ValueError(key)
        return algorithm(**kwargs)


## Algorithms pool initialization
AlgPool = AlgorithmPool()
AlgPool.submit(["RG", "RandomGreedy", "random_greedy", "randomgreedy"], RandomGreedy)
AlgPool.submit(["TS", "TabuSearch", "tabu_search", "tabusearch"], Tabu_Search)
AlgPool.submit(["LS", "LocalSearch", "local_search", "localsearch"], Local_Search)
AlgPool.submit(["SA", "SimulatedAnnealing", "simulated_annealing", "simulatedannealing"], Simulated_Annealing)
AlgPool.submit(["GA", "GeneticAlgorithm", "genetic_algorithm", "geneticalgorithm"], Genetic_Algorithm)
AlgPool.submit(["BS", "BinarySearch", "binary_search", "binarysearch"], BinarySearch)




