import pdb
from classes.Logger import Logger
from classes.System import System
from space4ai_parser.YamlGenerator import ParserJsonToYaml
from space4ai_parser.JsonGenerator import ParserYamlToJson
from classes.AlgorithmPool import AlgPool
import sys
import os
import json
import argparse
import numpy as np
import multiprocessing as mpp
from multiprocessing import Pool
import functools
from classes.Solution import Result, EliteResults
import time

#####################################

class MultiProcessing:

    ## @var starting_point
    # Some starting point(s) as initial solution(s)

    ## @var cpuCore
    # The Object of Logger.Logger type

    ## @var cpuCore
    # The number of cpu cores in the current machine

    ## MultiProcessing class constructor
    #   @param self The object pointer
    #   @param method A dictionary includes the name of algorithm and all the required parameters
    def __init__(self, method):
        self.method = method
        self.cpuCore = int(mpp.cpu_count())
        if "starting_point" in self.method["parameters"]:
            self.StartingPoints = self.method["parameters"]["starting_point"]
        else:
            self.StartingPoints = None
        self._core_params = self._get_core_params()
        self.logger = self.method["parameters"]["log"]

    ## Function to get a list whose n-th element stores a tuple with the number
    # of iterations to be performed by the n-th cpu core and the seed it should
    # use for random numbers generation
    #   @param iteration Total number of iterations to be performed
    #   @param seed Seed for random number generation
    #   @param cpuCore Total number of cpu cores
    #   @param logger Current logger
    #   @return The list of parameters required by each core (number of
    #           iterations, seed and logger)
    def _get_core_params(self):
        iteration = self.method["parameters"]["max_steps"]
        Max_time = self.method["parameters"]["max_time"]
        seed = self.method["parameters"]["seed"]
        logger = self.method["parameters"]["log"]
        core_params = []
        local_itr = int(iteration / self.cpuCore)
        remainder_itr = iteration % self.cpuCore
        local_Max_time = Max_time / self.cpuCore
        if self.StartingPoints:
            local_StartingPoints = int(len(self.StartingPoints) / self.cpuCore)
            remainder_StartingPoints = len(self.StartingPoints) % self.cpuCore
        next_idx = 0
        for r in range(self.cpuCore):
            if logger.stream != sys.stdout:
                if self.cpuCore > 1 and logger.verbose > 0:
                    log_file = ".".join(logger.stream.name.split(".")[:-1])
                    log_file += "_" + str(r) + ".log"
                else:
                    log_file = logger.stream.name
            else:
                log_file = ""
            r_seed = r * r * self.cpuCore * self.cpuCore * seed
            if r < remainder_itr:
                current_local_itr = local_itr + 1
            else:
                current_local_itr = local_itr

            if self.StartingPoints:
                if r < remainder_StartingPoints:
                    current_local_StartingPoints = local_StartingPoints + 1
                else:
                    current_local_StartingPoints = local_StartingPoints
                starting_points = self.StartingPoints[next_idx: next_idx + current_local_StartingPoints]
                next_idx = next_idx + current_local_StartingPoints
                core_params.append((current_local_itr, local_Max_time, r_seed, log_file, starting_points))
            else:
                core_params.append((current_local_itr, local_Max_time, r_seed, log_file))
        return core_params

    ## Method to run the algorithem on a specific core
    #   @param core_params The core params
    #   @param S The object of System.system
    #   @param method The dictionary includes the name and all required parameters of the method
    #   @return A list of results
    def run_alg(self, core_params, system_file, method):
        with open(system_file, "r") as a_file:
            json_object = json.load(a_file)
        S = System(system_json=json_object, log=self.method["parameters"]["log"])
        method["parameters"]["system"] = S
        method["parameters"]["seed"] = core_params[2]
        print("\n Seed: " + str(core_params[2]))
        print("\n Iteration number: " + str(core_params[0]))
        core_logger = method["parameters"]["log"]
        if core_params[3] != "":
            log_file = open(core_params[3], "a")
            core_logger.stream = log_file
            method["parameters"]["log"] = core_logger
        if self.StartingPoints:
            elite_sol = EliteResults(1, Logger(self.logger.stream,
                                               self.logger.verbose, self.logger.level + 1))
            elite_sol.elite_results.add(Result())
            if self.method["name"] in list(
                    i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["GA"]):
                if len(core_params[4]) > 0:
                    method["parameters"]["initial_state"] = core_params[4]
                    method["parameters"]["max_steps"] = core_params[0]
                    method["parameters"]["max_time"] = core_params[1]
                    algorithm = AlgPool.create(method["name"], **method["parameters"])
                    result = algorithm.run_algorithm()
                    elite_sol.add(result[0])
            else:
                for initial_state in core_params[4]:
                    method["parameters"]["initial_state"] = initial_state
                    method["parameters"]["max_steps"] = int(core_params[0] / len(core_params[4]))
                    method["parameters"]["max_time"] = core_params[1] / len(core_params[4])
                    algorithm = AlgPool.create(method["name"], **method["parameters"])
                    result = algorithm.run_algorithm()
                    elite_sol.add(result[0])
            results = elite_sol.elite_results[0], elite_sol
        else:

            method["parameters"]["max_steps"] = core_params[0]
            method["parameters"]["max_time"] = core_params[1]
            algorithm = AlgPool.create(method["name"], **method["parameters"])
            results = algorithm.run_algorithm()

        return results

    ## Method to run the algorithems in multi-processing manner
    #   @param json_object The json object of system json file
    #   @return 1) A boolean to show if the best solution is feasible
    #           2) A list of k_best solutions
    #           3) The result of the best solution
    #           4) The system object
    def run(self, system_file):
        #if self.method["name"] == "LS":

         #   results = self.run_alg(self._core_params[1], system_file, self.method)
          #  pdb.set_trace()
        solutions = []
        feasible_found = False
        elite_sol = []
        start = time.time()
        #if __name__ == "__main__":
        with Pool(processes=self.cpuCore) as pool:
            partial_gp = functools.partial(self.run_alg, system_file=system_file, method=self.method)
            full_result = pool.map(partial_gp, self._core_params)
        print("Multiprocessing ends.")
            #S = full_result[0][1]
        first_unfeasible = False
            # get final list combining the results of all threads
        for tid in range(self.cpuCore):
            if feasible_found:
                if full_result[tid][1].elite_results[0].performance[0]:
                    elite_sol.merge(full_result[tid][1], True)
            else:
                if full_result[tid][1].elite_results[0].performance[0]:
                    feasible_found = True
                    elite_sol = EliteResults(full_result[tid][1].K)
                    elite_sol.elite_results.add(Result())
                    elite_sol.add(full_result[tid][1].elite_results[0])
                else:
                    if not first_unfeasible:
                        elite_sol = EliteResults(full_result[tid][1].K)
                        elite_sol.elite_results.add(Result())
                        elite_sol.add(full_result[tid][1].elite_results[0], feasible_found)
                        first_unfeasible = True
                    else:
                        elite_sol.merge(full_result[tid][1], False)
            # if len(elite_sol.elite_results)<K:
            #    K=len(elite_sol.elite_results)

        if feasible_found:
            for sol in elite_sol.elite_results:
                if sol.cost < np.inf:
                    solutions.append(sol.solution)
        else:
            for sol in elite_sol.elite_results:
                if sol.violation_rate < np.inf:
                    solutions.append(sol.solution)

        return feasible_found, solutions, elite_sol.elite_results[0]


######################################################




## Function to create a directory given its name (if the directory already
# exists, nothing is done)
#   @param directory Name of the directory to be created
def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " + directory)


## Function to create the pure json file from system description by removing
# the comments lines
#   @param main_json_file Name of the file with the system description
#   @return Name of the pure json file with the system description
def create_pure_json(main_json_file):

    data = []

    # read file
    with open(main_json_file,"r") as fp:
        Lines = fp.readlines()

    # loop over lines and drop portions preceded by '#'
    for line in Lines:
        idx = line.find('#')
        if (idx != -1):
            last = line[-1]
            line = line[0:idx]
            if last == "\n":
                line = line+last
        data.append(line)

    # write data on the new json file
    pure_json = 'pure_json.json'
    filehandle = open(pure_json, "w")
    filehandle.writelines(data)
    filehandle.close()

    return pure_json


## Function to generate the output json file as the optimal placement for a
# specified Lambda
#   @param Lambda incoming workload rate
#   @param result The optimal Solution.Result returned by fun_greedy
#   @param S An instance of System.System class including system description
#   @param onFile True if the result should be printed on file (default: True)
def generate_output_json(Lambda, result, S, onFile = True):

    # generate name of output file (if required)
    if onFile:
        output_json = "Output_Files/Lambda_" + \
                        str(round(float(Lambda), 10)) + \
                            '_output_json.json'
    else:
        output_json = ""

    result.print_result(S, solution_file=output_json)


def main(application_dir):
    error = Logger(stream=sys.stderr, verbose=1, error=True)
    parser_json_generator = ParserYamlToJson(application_dir, "s4aid")
    input_json_dir = parser_json_generator.make_input_json()
    system_file = parser_json_generator.make_system_file()
    dep_list = []


    #system_file = "/home/SPACE4AI/Output_Files/paper_results/with_branches/light_cons/Output_Files_1min_hyp_heu/large_scale/15Components/Ins1/system_description.json"#"/Users/hamtasedghani/Downloads/Video_search/space4ai-d/SystemFile.json"#"/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/light_cons/Output_Files_1min_hyp_heu/large_scale/15Components/Ins1/system_description.json"
    #system_file = "/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/strict_cons/Output_Files_10min_hyp_heu/large_scale/7Components/Ins6/system_description.json"
    with open(input_json_dir, "r") as a_file:
        input_json = json.load(a_file)
    #input_json = json.loads(input_json)
    RG_method = {}
    if "VerboseLevel" in input_json.keys():
        logger = Logger(stream=sys.stderr, verbose=input_json["VerboseLevel"])
    else:
        error.log("{} does not exist.".format("VerboseLevel"))
        sys.exit(1)
    if "Methods" in input_json.keys():
        Methods = input_json["Methods"]
        RG_list = list(i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["RG"])
        RG_method_list = [(key, Methods[key]) for key in Methods if Methods[key]["name"] in RG_list]
        if len(RG_method_list) > 0:
            RG = RG_method_list[0][1]
            key = RG_method_list[0][0]
            Methods.pop(key)
            RG_method["name"] = RG["name"]
            RG_method["parameters"] = {}
            if "iterations" in RG:
                RG_method["parameters"]["max_steps"] = RG["iterations"]
            if "duration" in RG:
                RG_method["parameters"]["max_time"] = RG["duration"]
            if "iterations" not in RG and "duration" not in RG:
                error.log("At least one of duration or iterations should be specified for RG ")
                sys.exit(1)
            if "Seed" in input_json.keys():
                RG_method["parameters"]["seed"] = input_json["Seed"]
            else:
                error.log("{} does not exist".format("Seed"))
                sys.exit(1)
        else:
            error.log("Random Greedy is a mandatory method and the name can be one of this list: {}.".format(RG_list))
            sys.exit(1)
    else:
        error.log("{} does not exist".format("Methods"))
        sys.exit(1)
    startingPointNumber = 1

    BS_list = list(i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["BS"])
    BS_method_list = [(key, Methods[key]) for key in Methods if Methods[key]["name"] in BS_list]
    if len(BS_method_list) > 0:
        BS = BS_method_list[0][1]
        Methods.pop(BS_method_list[0][0])
        BS_method = {}
        BS_method["name"] = "BS"
        if "upperBoundLambda" in BS:
            upper_bound_lambda = BS["upperBoundLambda"]
        else:
            error.log("upperBoundLambda is mandatory parameter for BS")
            sys.exit(1)
        if "epsilon" in BS:
            epsilon = BS["epsilon"]
        else:
            error.log("epsilon is mandatory parameter for BS")
            sys.exit(1)
    else:
        error.log("Binary Search is a mandatory method and the name can be one of this list: {}.".format(BS_list))
        sys.exit(1)

    Heu_method = {}
    if len(Methods.keys()) > 0:
        Heu_method["parameters"] = {}
        heu_list = list(i for i in AlgPool.algorithms if AlgPool.algorithms[i] in [AlgPool.algorithms["LS"],
                                                                                   AlgPool.algorithms["TS"],
                                                                                   AlgPool.algorithms["SA"],
                                                                                   AlgPool.algorithms["GA"]])
        Heu_method_list = [(key, Methods[key]) for key in Methods if Methods[key]["name"] in heu_list]
        if len(Heu_method_list) > 0:
            Heu = Heu_method_list[0][1]
            Methods.pop(Heu_method_list[0][0])
            if Heu["name"] in heu_list:
                Heu_method["name"] = Heu["name"]
            else:
                error.log("Heuristic name should be  one of this list: {}.".format(heu_list))
                sys.exit(1)
            if "iterations" in Heu:
                Heu_method["parameters"]["max_steps"] = Heu["iterations"]
            if "duration" in Heu:
                Heu_method["parameters"]["max_time"] = Heu["duration"]
            if "iterations" not in Heu and "duration" not in Heu:
                error.log("At least one of duration or iterations should be specified for heuristic.")
                sys.exit(1)
            Heu_method["parameters"]["seed"] = RG_method["parameters"]["seed"]
            if "startingPointNumber" in Heu:
                startingPointNumber = Heu["startingPointNumber"]
            else:
                error.log(" startingPointNumber should be specified")
                sys.exit(1)
#################### Special parameters #######################################
        ############ LS parameters #####################
            if Heu_method["name"] in list(i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["LS"]):
                if "specialParameters" in Heu:
                    if "minScore" not in Heu["specialParameters"]:
                        print("minScore is optional fild for Local Search. The default value is None.")
                    else:
                        Heu_method["parameters"]["max_score"] = Heu["specialParameters"]["minScore"]

            ############ TS parameters #####################
            elif Heu_method["name"] in list(
                    i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["TS"]):
                if "tabuSize" in Heu["specialParameters"]:
                    Heu_method["parameters"]["tabu_size"] = Heu["specialParameters"]["tabuSize"]
                else:
                    error.log(" tabuSize should be specified")
                    sys.exit(1)
                if "minScore" not in Heu["specialParameters"]:
                    print("minScore is optional fild for Tabu Search. The default value is None.")
                else:
                    Heu_method["parameters"]["max_score"] = Heu["specialParameters"]["minScore"]
            ############## SA parameters #####################
            elif Heu_method["name"] in list(
                    i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["SA"]):
                if "tempBegin" in Heu["specialParameters"]:
                    Heu_method["parameters"]["temp_begin"] = Heu["specialParameters"]["tempBegin"]
                else:
                    error.log(" tempBegin, which is the initial temperature, should be specified")
                    sys.exit(1)
                if "scheduleConstant" in Heu["specialParameters"]:
                    Heu_method["parameters"]["schedule_constant"] = Heu["specialParameters"]["scheduleConstant"]
                else:
                    error.log(" scheduleConstant, which is the annealing constant to reduce the temperature, should be specified")
                    sys.exit(1)
                if "minEnergy" not in Heu["specialParameters"]:
                    print("minEnergy is optional fild for Local Search. The initial value is None.")
                else:
                    Heu_method["parameters"]["min_energy"] = Heu["specialParameters"]["minEnergy"]
                if "schedule" in Heu["specialParameters"]:
                    Heu_method["parameters"]["schedule"] = Heu["specialParameters"]["schedule"]
                else:
                    error.log("schedule, which specifies the annealing schedule method, should be specified. it can be 'exponential' or 'linear'")
                    sys.exit(1)
            ############## GA parameters #####################
            else:
                if "crossoverRate" in Heu["specialParameters"]:
                    Heu_method["parameters"]["crossover_rate"] = Heu["specialParameters"]["crossoverRate"]
                else:
                    error.log("crossoverRate is a mandatory parameter for GA and it should be specified")
                    sys.exit(1)
                if "mutationRate" in Heu["specialParameters"]:
                    Heu_method["parameters"]["mutation_rate"] = Heu["specialParameters"]["mutationRate"]
                else:
                    error.log(" mutationRate is a mandatory parameter for GA and it should be specified")
                    sys.exit(1)
                if "minFitness" not in Heu["specialParameters"]:
                    print("minFitness is optional fild for Local Search. The initial value is None.")
                else:
                    Heu_method["parameters"]["min_fitness"] = Heu["specialParameters"]["minFitness"]
            Heu_method["parameters"]["log"] = logger
    RG_method["parameters"]["k_best"] = startingPointNumber
    RG_method["parameters"]["log"] = logger


    print("\nStart parsing YAML files... ")
    #parser_s4aid = ParserJsonToYaml(application_dir,"s4air","space4ai-r/deployment1")
    #parser_s4aid.main_function()

    #system_file = create_pure_json(system_file)
    with open(system_file, "r") as a_file:
        json_object = json.load(a_file)

    MP = MultiProcessing(RG_method)
    feasible_found, solutions, result = MP.run(system_file)
    #feasibility, starting_points, result, S = Random_Greedy_run(json_object, method1)
    if not feasible_found:
        error.log("No feasible solution is found by RG")
    else:
        if Heu_method != {}:
            Heu_method["parameters"]["starting_point"] = solutions
            MP = MultiProcessing(Heu_method)
            feasible_found, solutions, result = MP.run(system_file)
    output_json=application_dir+"/space4ai-d/Output.json"
    if result.solution is None:
        print("No solution is found.")
    else:
        Y_hat = result.solution.Y_hat
        S = System(system_json=json_object, log=logger)
        result.print_result(S, output_json)
        parser_yaml_generator = ParserJsonToYaml(application_dir, "s4aid")
        parser_yaml_generator.main_function()
        ################### find highest Lambda #######################
        BS_method["parameters"] = {}
        BS_method["parameters"][
            "solution_file"] = output_json  # "/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/strict_cons/Output_Files_1min_hyp_heu/large_scale/10Components/Ins6/best_Tabu_0.45.json"
        if parser_json_generator.is_degraded(parser_json_generator.common_config_path):
            dep_list=parser_json_generator.get_alternative_list(application_dir)
        else:
            dep_list = ["original_deployment"]
        for dep_name in dep_list:

            parser_json_alt_generator = ParserYamlToJson(application_dir, "s4aid", alternative_deployment=dep_name)
            system_file = parser_json_alt_generator.make_system_file()
            BS_method["parameters"]["system"] = System(system_file=system_file)
            BS_method["parameters"]["system_file"] = system_file
            algorithm = AlgPool.create(BS_method["name"], **BS_method["parameters"])
            result, highest_feasible_lambda = algorithm.run_algorithm(upper_bound_lambda, epsilon, Y_hat=Y_hat)
            output_json_max_lambda = application_dir + "/space4ai-r/Output_max_Lambda_" + dep_name + ".json"
            result.print_result(algorithm.system, output_json_max_lambda)

        ################### find highest Lambda #######################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SPACE4AI-D")

    parser.add_argument("-C", "--application_dir",
                        help="Application directory")
    
    args = parser.parse_args()

    # initialize error stream
    error = Logger(stream=sys.stderr, verbose=1, error=True)
    dic = {}
    # check if the system configuration file exists
    if not os.path.exists(args.application_dir):
        error.log("{} does not exist".format(args.application_dir))
        sys.exit(1)
    else:
        application_dir = args.application_dir


    #application_dir="/Users/hamtasedghani/space4ai-d/filter_classifier_degraded_performance/step_4"
    #error = Logger(stream = sys.stderr, verbose=1, error=True)
    main(application_dir)
