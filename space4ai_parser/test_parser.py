from JsonGenerator import ParserYamlToJson
from YamlGenerator import ParserJsonToYaml
from Logger import Logger

import argparse
import os


def parse_arguments() -> argparse.Namespace:
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(
        description="Testing the SPACE4AI Parser"
    )
    parser.add_argument(
      "--test", 
      help="Keyword identifying the test to be performed", 
      type=str,
      choices=[
          "s4aid_input", 
          "s4aid_output", 
          "s4air_input", 
          "s4air_output"
        ],
    )
    parser.add_argument(
      "--application_dir", 
      help="Name of the application directory", 
      type=str
    )
    parser.add_argument(
      "--alternative_deployment", 
      help="Name of the alternative deployment with degraded performance", 
      type=str,
      default=None
    )
    parser.add_argument(
      "--only_edge", 
      help="True if only edge resources should be considered", 
      default=False,
      action="store_true"
    )
    parser.add_argument(
      "--verbosity_level", 
      help="Verbosity level for logging", 
      type=int, 
      default=0
    )
    args, _ = parser.parse_known_args()
    return args


def generate_s4aid_input(
        application_dir: str, 
        alternative_deployment: str, 
        logger: Logger
    ) -> str:
    logger.log("Generate space4ai-d input json files")
    # initialize parser
    parser = ParserYamlToJson(
        application_dir, 
        "space4ai-d", 
        alternative_deployment=alternative_deployment, 
        log=logger
    )
    # generate input json file
    input_json_dir = parser.make_input_json()
    # generate system file
    system_file = parser.make_system_file()
    return input_json_dir, system_file


def generate_s4aid_output(
        application_dir: str, 
        alternative_deployment: str, 
        logger: Logger
    ) -> str:
    logger.log("Generate space4ai-d output yaml file")
    output_yaml = os.path.join(
        application_dir, 
        "aisprint/deployments/optimal_deployment/production_deployment.yaml"
    )
    # initialize parser
    parser = ParserJsonToYaml(
        application_dir, 
        "space4ai-d", 
        alternative_deployment=alternative_deployment, 
        log=logger
    )
    # generate output
    parser.main_function()
    return output_yaml


def generate_s4air_input(
        application_dir: str, 
        alternative_deployment: str, 
        only_edge: bool, 
        logger: Logger
    ) -> str:
    logger.log("Generate space4ai-r input json files")
    # initialize parser
    parser = ParserYamlToJson(
        application_dir, 
        "space4ai-r", 
        alternative_deployment=alternative_deployment, 
        log=logger, 
        only_edge=only_edge
    )
    # generate system file
    system_file = parser.make_system_file()
    # read current production deployment
    current_solution = parser.make_current_solution()
    if only_edge:
        current_solution, _, _ = parser.filter_current_solution()
    return system_file, current_solution


def generate_s4air_output(
        application_dir: str, 
        alternative_deployment: str, 
        logger: Logger
    ) -> str:
    logger.log("Generate space4ai-r output yaml file")
    output_yaml = os.path.join(
        application_dir, 
        "aisprint/deployments/optimal_deployment/production_deployment.yaml"
    )
    # initialize parser
    parser = ParserJsonToYaml(
        application_dir, 
        "space4ai-r", 
        alternative_deployment=alternative_deployment, 
        log=logger
    )
    # generate output
    parser.main_function()
    return output_yaml


def main():
    # parse arguments
    args = parse_arguments()
    test = args.test
    application_dir = args.application_dir
    alternative_deployment = args.alternative_deployment
    only_edge = args.only_edge
    verbosity_level = args.verbosity_level
    # initialize logger
    logger = Logger(verbose=verbosity_level)
    # generate space4ai-d json input file
    if test == "s4aid_input":
        generate_s4aid_input(application_dir, alternative_deployment, logger)
    # generate space4ai-d yaml output file
    elif test == "s4aid_output":
        generate_s4aid_output(application_dir, alternative_deployment, logger)
    # generate space4ai-r json input file
    elif test == "s4air_input":
        generate_s4air_input(
            application_dir, 
            alternative_deployment, 
            only_edge, 
            logger
        )
    # generate space4ai-r yaml output file
    elif test == "s4air_output":
        generate_s4air_output(application_dir, alternative_deployment, logger)


if __name__ == "__main__":
    main()
