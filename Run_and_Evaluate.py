from external import space4ai_logger

import pdb
from classes.System import System
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
import pathlib


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
            if logger.out_stream != sys.stdout:
                if self.cpuCore > 1 and logger.verbose > 0:
                    log_file = ".".join(logger.out_stream.name.split(".")[:-1])
                    log_file += "_" + str(r) + ".log"
                else:
                    log_file = logger.out_stream.name
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
        Lambda = json_object["Lambda"]
        method["parameters"]["seed"] = core_params[2]
        core_logger = method["parameters"]["log"]
        if core_params[3] != "":
            log_file = open(core_params[3], "a")
            core_logger.out_stream = log_file
            method["parameters"]["log"] = core_logger
        core_logger.log("Seed: " + str(core_params[2]))
        core_logger.log("Iteration number: " + str(core_params[0]))
        S = System(system_json=json_object, log=core_logger)
        method["parameters"]["system"] = S
        if self.StartingPoints:
            elite_sol = EliteResults(
                1, 
                space4ai_logger.Logger(
                    name="SPACE4AI-D",
                    out_stream=core_logger.out_stream,
                    verbose=core_logger.verbose
                )
            )
            elite_sol.elite_results.add(Result(core_logger))
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
        # if self.method["name"] == "LS":

        #   results = self.run_alg(self._core_params[1], system_file, self.method)
        #  pdb.set_trace()
        solutions = []
        feasible_found = False
        elite_sol = []
        start = time.time()
        if __name__ == "__main__":
            with Pool(processes=self.cpuCore) as pool:
                partial_gp = functools.partial(self.run_alg, system_file=system_file, method=self.method)
                full_result = pool.map(partial_gp, self._core_params)
            print("Multiprocessing ends.")
            # S = full_result[0][1]
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
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Creating directory. " + directory)


## Function to create the pure json file from system description by removing
# the comments lines
#   @param main_json_file Name of the file with the system description
#   @return Name of the pure json file with the system description
def create_pure_json(main_json_file):
    data = []

    # read file
    with open(main_json_file, "r") as fp:
        Lines = fp.readlines()

    # loop over lines and drop portions preceded by '#'
    for line in Lines:
        idx = line.find('#')
        if (idx != -1):
            last = line[-1]
            line = line[0:idx]
            if last == "\n":
                line = line + last
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
def generate_output_json(system_file, Lambda, result, logger, onFile=True):
    # generate name of output file (if required)
    if onFile:
        path = pathlib.Path(system_file).parent.resolve()
        output_json = str(path) + "/Lambda_" + str(round(float(Lambda), 4)) + ".json"
    else:
        output_json = ""
    # load system
    with open(system_file, 'r') as f:
        data = json.load(f)
    data["Lambda"] = Lambda
    S = System(system_json=data, log=logger)
    # print
    result.print_result(S, solution_file=output_json)


def main(dic, logger, log_directory):
    system_file = dic["system_file"]
    Lambda_list = []

    # system_file = "/home/SPACE4AI/Output_Files/paper_results/with_branches/light_cons/Output_Files_1min_hyp_heu/large_scale/15Components/Ins1/system_description.json"#"/Users/hamtasedghani/Downloads/Video_search/space4ai-d/SystemFile.json"#"/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/light_cons/Output_Files_1min_hyp_heu/large_scale/15Components/Ins1/system_description.json"
    # system_file = "/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/strict_cons/Output_Files_10min_hyp_heu/large_scale/7Components/Ins6/system_description.json"

    # input_json = json.loads(input_json)
    RG_method = {}

    ##############################
    with open(system_file, "r") as a_file:
        json_object = json.load(a_file)
    # evaluate-only scenario
    if not "config_file" in dic.keys():
        # get solution file and lambda
        solution_file = dic["solution_file"]
        Lambda = dic["Lambda"]
        json_object["Lambda"] = Lambda
        # set logger stream (if printing on file)
        if log_directory != "":
            log_file_lambda = "LOG_" + str(Lambda) + ".log"
            log_file_lambda = os.path.join(log_directory, log_file_lambda)
            log_file_lambda = open(log_file_lambda, "a")
            logger.out_stream = log_file_lambda
        logger.log(f"Current Lambda is {Lambda} req/s")
        logger.log(f"Solution will be printed on {solution_file}")
        # initialize system
        S = System(system_json=json_object, log=logger)
        # Create a fake LS to use create_solution_by_file function
        RG_method = {}
        RG_method["name"] = "LS"
        RG_method["parameters"] = {}
        RG_method["parameters"]["max_steps"] = 1
        RG_method["parameters"]["max_time"] = 1
        RG_method["parameters"]["seed"] = 1
        RG_method["parameters"]["k_best"] = 1
        RG_method["parameters"]["log"] = logger
        RG_method["parameters"]["initial_state"] = 1
        RG_method["parameters"]["system"] = S
        algorithm = AlgPool.create(RG_method["name"], **RG_method["parameters"])
        result = algorithm.create_solution_by_file(solution_file)
        path = pathlib.Path(system_file).parent.resolve()
        output_json = str(path) + "/Lambda_" + str(Lambda) + ".json"
        if result.solution is None:
            logger.log("No solution is found.")
        else:
            result.print_result(S, output_json)
    # run-algorithms scenario
    else:
        # load configuration
        config_file = dic["config_file"]
        with open(config_file, "r") as a_file:
            input_json = json.load(a_file)
        if "LambdaBound" in input_json.keys():
            start_lambda = input_json["LambdaBound"]["start"]
            end_lambda = input_json["LambdaBound"]["end"]
            step = input_json["LambdaBound"]["step"]
            for Lambda in np.arange(start_lambda, end_lambda, step):
                Lambda_list.append(Lambda)
        if "VerboseLevel" in input_json.keys():
            verbose = input_json["VerboseLevel"]
            logger.log(f"Using verbosity level from configuration file ({verbose})")
        elif "VerboseLevel" in dic.keys():
            verbose = dic["VerboseLevel"]
            logger.log(f"Using verbosity level provided at command-line ({verbose})")
        else:
            logger.err("{} does not exist.".format("VerboseLevel"))
            sys.exit(1)
        if "Lambda" in dic.keys():
            Lambda_list.append(dic["Lambda"])

        if len(Lambda_list) == 0:
            logger.err(
                "No values provided for Lambda. Specify -L from the command-line or LambdaBound in the config file"
            )
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
                    logger.err("At least one of duration or iterations should be specified for RG ")
                    sys.exit(1)
                if "Seed" in input_json.keys():
                    RG_method["parameters"]["seed"] = input_json["Seed"]
                else:
                    logger.err("{} does not exist".format("Seed"))
                    sys.exit(1)
            else:
                logger.err(
                    "Random Greedy is a mandatory method and the name can be one of this list: {}.".format(RG_list)
                )
                sys.exit(1)
        else:
            logger.err("{} does not exist".format("Methods"))
            sys.exit(1)
        startingPointNumber = 1

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
                    logger.err("Heuristic name should be  one of this list: {}.".format(heu_list))
                    sys.exit(1)
                if "iterations" in Heu:
                    Heu_method["parameters"]["max_steps"] = Heu["iterations"]
                if "duration" in Heu:
                    Heu_method["parameters"]["max_time"] = Heu["duration"]
                if "iterations" not in Heu and "duration" not in Heu:
                    logger.err("At least one of duration or iterations should be specified for heuristic.")
                    sys.exit(1)
                Heu_method["parameters"]["seed"] = RG_method["parameters"]["seed"]
                if "startingPointNumber" in Heu:
                    startingPointNumber = Heu["startingPointNumber"]
                else:
                    logger.err(" startingPointNumber should be specified")
                    sys.exit(1)
                #################### Special parameters #######################################
                ############ LS parameters #####################
                if Heu_method["name"] in list(
                        i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["LS"]):
                    if "specialParameters" in Heu:
                        if "minScore" not in Heu["specialParameters"]:
                            logger.log("minScore is optional fild for Local Search. The default value is None.")
                        else:
                            Heu_method["parameters"]["max_score"] = Heu["specialParameters"]["minScore"]

                ############ TS parameters #####################
                elif Heu_method["name"] in list(
                        i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["TS"]):
                    if "tabuSize" in Heu["specialParameters"]:
                        Heu_method["parameters"]["tabu_size"] = Heu["specialParameters"]["tabuSize"]
                    else:
                        logger.err(" tabuSize should be specified")
                        sys.exit(1)
                    if "minScore" not in Heu["specialParameters"]:
                        logger.log("minScore is optional fild for Tabu Search. The default value is None.")
                    else:
                        Heu_method["parameters"]["max_score"] = Heu["specialParameters"]["minScore"]
                ############## SA parameters #####################
                elif Heu_method["name"] in list(
                        i for i in AlgPool.algorithms if AlgPool.algorithms[i] == AlgPool.algorithms["SA"]):
                    if "tempBegin" in Heu["specialParameters"]:
                        Heu_method["parameters"]["temp_begin"] = Heu["specialParameters"]["tempBegin"]
                    else:
                        logger.err(" tempBegin, which is the initial temperature, should be specified")
                        sys.exit(1)
                    if "scheduleConstant" in Heu["specialParameters"]:
                        Heu_method["parameters"]["schedule_constant"] = Heu["specialParameters"]["scheduleConstant"]
                    else:
                        logger.err(
                            " scheduleConstant, which is the annealing constant to reduce the temperature, should be specified"
                        )
                        sys.exit(1)
                    if "minEnergy" not in Heu["specialParameters"]:
                        logger.log("minEnergy is optional fild for Local Search. The initial value is None.")
                    else:
                        Heu_method["parameters"]["min_energy"] = Heu["specialParameters"]["minEnergy"]
                    if "schedule" in Heu["specialParameters"]:
                        Heu_method["parameters"]["schedule"] = Heu["specialParameters"]["schedule"]
                    else:
                        logger.err(
                            "schedule, which specifies the annealing schedule method, should be specified. it can be 'exponential' or 'linear'"
                        )
                        sys.exit(1)
                ############## GA parameters #####################
                else:
                    if "crossoverRate" in Heu["specialParameters"]:
                        Heu_method["parameters"]["crossover_rate"] = Heu["specialParameters"]["crossoverRate"]
                    else:
                        logger.err("crossoverRate is a mandatory parameter for GA and it should be specified")
                        sys.exit(1)
                    if "mutationRate" in Heu["specialParameters"]:
                        Heu_method["parameters"]["mutation_rate"] = Heu["specialParameters"]["mutationRate"]
                    else:
                        logger.err(" mutationRate is a mandatory parameter for GA and it should be specified")
                        sys.exit(1)
                    if "minFitness" not in Heu["specialParameters"]:
                        logger.log("minFitness is optional fild for Local Search. The initial value is None.")
                    else:
                        Heu_method["parameters"]["min_fitness"] = Heu["specialParameters"]["minFitness"]
        RG_method["parameters"]["k_best"] = startingPointNumber
        for Lambda in Lambda_list:
            with open(system_file, 'r') as f:
                data = json.load(f)
            data["Lambda"] = Lambda
            system = json.dumps(data, indent=2)
            with open(system_file, "w") as f:
                f.write(system)
            # set logger stream for the current load
            if log_directory != "":
                log_file_lambda = "LOG_" + str(round(float(Lambda), 4)) + ".log"
                log_file_lambda = os.path.join(log_directory, log_file_lambda)
                log_file_lambda = open(log_file_lambda, "a")
            else:
                log_file_lambda = sys.stdout
            lambda_logger = space4ai_logger.Logger(
                name=logger.name, 
                out_stream=log_file_lambda, 
                verbose=logger.verbose
            )
            logger.log(f"Current Lambda is {Lambda} req/s")
            # set logger for methods
            if len(Heu_method) > 0:
                Heu_method["parameters"]["log"] = lambda_logger
            RG_method["parameters"]["log"] = lambda_logger
            # initialize multiprocessing
            MP = MultiProcessing(RG_method)
            feasible_found, solutions, result = MP.run(system_file)
            # feasibility, starting_points, result, S = Random_Greedy_run(json_object, method1)
            if not feasible_found:
                logger.err("No feasible solution is found by RG")
            else:
                if Heu_method != {}:
                    Heu_method["parameters"]["starting_point"] = solutions
                    MP = MultiProcessing(Heu_method)
                    feasible_found, solutions, result = MP.run(system_file)
            # print result
            if result.solution is None:
                lambda_logger.log("No solution is found.")
            else:
                generate_output_json(system_file, Lambda, result, logger)
            if log_directory != "":
                log_file_lambda.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SPACE4AI-D")

    parser.add_argument("-s", "--system_file",
                        help="System configuration file")
    parser.add_argument('-L', "--Lambda",
                        help="Lambda")
    parser.add_argument('-c', "--config",
                        help="Test configuration file",
                        default="ConfigFiles/Input_file.json")
    parser.add_argument('-v', "--verbose",
                        help="Verbosity level",
                        type=int,
                        default=0)
    parser.add_argument('-e', '--evaluation_lambda', nargs=2, help="Evaluate the solution with Lambda")
    parser.add_argument('-l', "--log_directory",
                        help="Directory for logging",
                        default="")

    args = parser.parse_args()

    # initialize logger
    logger = space4ai_logger.Logger(
        name="SPACE4AI-D",
        verbose=args.verbose
    )

    # check if the log directory exists and create it otherwise
    if args.log_directory != "":
        if os.path.exists(args.log_directory):
            print("Directory {} already exists. Terminating...". \
                  format(args.log_directory))
            sys.exit(0)
        else:
            createFolder(args.log_directory)
        general_log_file = os.path.join(args.log_directory, "LOG.log")
        general_log_file = open(general_log_file, "a")
        logger.out_stream = general_log_file

    dic = {}
    # check if the system configuration file exists
    if not os.path.exists(args.system_file):
        logger.err("{} does not exist".format(args.system_file))
        sys.exit(1)
    else:
        dic["system_file"] = args.system_file
    # check if the test configuration file exists or we need an evaluation
    if args.evaluation_lambda is None:
        if not os.path.exists(args.config):
            logger.err("{} does not exist".format(args.config))
            sys.exit(1)
        else:
            dic["config_file"] = args.config

            if args.Lambda is not None:
                dic["Lambda"] = float(args.Lambda)
        dic["VerboseLevel"] = args.verbose
    else:
        try:
            Lambda = float(args.evaluation_lambda[0])
            dic["Lambda"] = Lambda
        except ValueError:
            logger.err("{} must be a number".format(args.evaluation_lambda[0]))
            sys.exit(1)
        if not os.path.exists(args.evaluation_lambda[1]):
            logger.err("{} (solution to evaluate) does not exist".format(args.evaluation_lambda[1]))
            sys.exit(1)
        else:
            solution_file = args.evaluation_lambda[1]
            dic["solution_file"] = solution_file
            dic["VerboseLevel"] = args.verbose

    main(dic, logger, args.log_directory)

    if args.log_directory != "":
        general_log_file.close()

    '''dic = {}
    dic["system_file"] = "/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/SPACE4AI-D-AllMaterials/SPACE4AI-D-Zenodo/Heuristics_HyperOpt/Output_Files/strict_cons/Output_Files_1min_hyp_heu/large_scale/7Components/Ins1/system_description.json"
    #dic["config_file"] = "ConfigFiles/Input_file.json"
    dic["Lambda"] = 1
    dic["VerboseLevel"] = 1
    dic["solution_file"] = "/Users/hamtasedghani/space4ai-d/Output_Files/paper_results/with_branches/SPACE4AI-D-AllMaterials/SPACE4AI-D-Zenodo/Heuristics_HyperOpt/Output_Files/strict_cons/Output_Files_1min_hyp_heu/large_scale/7Components/Ins1/Output.json"
    createFolder("log_dir")
    main(dic, "log_dir")'''
