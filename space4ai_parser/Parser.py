from Logger import Logger

import yaml
import sys
import os
import re

class Parser:

    def __init__(
            self, 
            application_dir, 
            who, 
            alternative_deployment=None, 
            log=Logger()
        ):
        """
        Initialize parser
        """
        # initialize loggers
        self.logger = log
        self.error = Logger(stream=sys.stderr, verbose=1, error=True)

        # initialize application directory and the common subdirectories
        self.initialize_common_paths(application_dir)
        
        # check if the application supports degraded-performance components
        self.degraded = self.is_degraded(self.common_config_path)

        # check if an alternative deployment is specified in case components 
        # admit degraded performance
        if self.degraded:
            if alternative_deployment is not None:
                self.logger.log(
                    f"Considering deployment {alternative_deployment}"
                )
                self.alternative_deployment = alternative_deployment
            else:
                self.logger.log(
                    "No alternative deployment specified. Using original"
                )
                self.alternative_deployment = "original_deployment"

        # check who is the caller
        if who.lower() in ["s4ai-d", "space4ai-d", "s4aid", "space4aid"]:
            self.who = "SPACE4AI-D"
            self.current_deployment_path = self.optimal_deployment_path
        elif who.lower() in ["s4ai-r", "space4ai-r", "s4air", "space4air"]:
            self.who = "SPACE4AI-R"
            # the space4ai-r subdirectory needs to be generated
            if not os.path.exists(self.space4air_path):
                os.mkdir(self.space4air_path)
            # space4ai-r assumes that the current production deployment 
            # already exists; uses the optimal deployment otherwise
            folder_path = "aisprint/deployments/current_deployment"
            self.current_deployment_path = os.path.join(
                application_dir, folder_path
            )
            if not os.path.exists(self.current_deployment_path):
                self.logger.warn(
                    "Directory {} does not exist; using optimal deployment".\
                        format(self.current_deployment_path)
                )
                self.current_deployment_path = self.optimal_deployment_path
        else:
            self.error.log(
                "The role of parser's user must be specified, s4ai-d or s4ai-r"
            )
            sys.exit(1)
        
        # generate the list of component, deployment and partition names
        self.get_names_to_code()
    
    def initialize_common_paths(self, application_dir):
        self.application_dir = application_dir
        folder_path = "common_config"
        self.common_config_path = os.path.join(
            application_dir, folder_path
        )
        folder_path = "aisprint/designs"
        self.component_partitions_path = os.path.join(
            application_dir, folder_path
        )
        folder_path = "space4ai-d"
        self.space4aid_path = os.path.join(
            application_dir, folder_path
        )
        folder_path = "oscarp"
        self.oscarp_path = os.path.join(
            application_dir, folder_path
        )
        folder_path = "space4ai-r"
        self.space4air_path = os.path.join(
            application_dir, folder_path
        )
        folder_path = "aisprint/deployments/optimal_deployment"
        self.optimal_deployment_path = os.path.join(
            application_dir, folder_path
        )

    def get_names_to_code(self):
        """
        Generate the components/deployments/partitions names used in the code 
        from the original ones (e.g., the first partition in the first 
        deployment of the first component will be identified by h1 of s1 of c1)
        """
        # load the candidate deployments
        filename = "candidate_deployments.yaml"
        filepath = os.path.join( self.common_config_path, filename)
        with open(filepath) as file:
            candidate_deployments = yaml.full_load(file)["Components"]
        # load the yaml file with info about the component partitions
        filename = "component_partitions.yaml"
        filepath = os.path.join(self.component_partitions_path, filename)
        with open(filepath) as file:
            components = yaml.full_load(file)["components"]
        # generate dictionary
        self.names_to_code = {}
        self.code_to_names = {}
        for key, value in candidate_deployments.items():
            component_name = value["name"]
            self.logger.log(f"Reading {key} with name {component_name}", 5)
            self.names_to_code[component_name] = {}
            # loop over component partitions if components with degraded 
            # performance are not allowed
            if not self.degraded:
                if component_name in components.keys():
                    c = key.replace("component", "")
                    partitions = components[component_name]["partitions"]
                    h = 0
                    for partition in sorted(partitions):
                        self.logger.log(f"Examining partition {partition}", 5)
                        h += 1
                        if partition != "base":
                            tokens = partition.strip("partition").split('_')
                            s = int(tokens[0]) + 1
                            component_key = f"{key}_partitionX_{tokens[1]}"
                        else:
                            s = 1
                            component_key = key
                        # saving names
                        c_code = "c" + str(c)
                        s_code = "s" + str(s)
                        h_code = "h" + str(h)
                        self.names_to_code[component_name][partition] = {
                            "c": c_code,
                            "s": s_code,
                            "h": h_code
                        }
                        self.code_to_names[(c_code, s_code, h_code)] = {
                            "key": component_key,
                            "component": component_name,
                            "partition": partition
                        }
            # otherwise, save info about the component and its alternatives
            else:
                if not "_alternative" in key:
                    c_code = key.replace("omponent", "")
                else:
                    tokens = key.split("_alternative")
                    c = tokens[0].replace("component", "")
                    c_code = "c" + str(c) + "_" + str(tokens[1])
                s_code = "s1"
                h_code = "h1"
                partition = "base"
                self.names_to_code[component_name][partition] = {
                    "c": c_code,
                    "s": s_code,
                    "h": h_code
                }
                self.code_to_names[(c_code, s_code, h_code)] = {
                    "key": key,
                    "component": component_name,
                    "partition": "base"
                }

    def get_selected_resources(self, production_deployment):
        """
        Get the dictionary of computational layers and resources selected 
        in the production deployment
        """
        # get the selected network domains from the production deployment
        NDs = production_deployment["NetworkDomains"]
        # loop over all network domains
        selected_resources = {}
        resources_data = {}
        for ND_data in NDs.values():
            if "ComputationalLayers" in ND_data.keys():
                # loop over all computational layers in the domain and get 
                # the list of corresponding resources
                for layer,layer_data in ND_data["ComputationalLayers"].items():
                    selected_resources[layer] = []
                    resources_data[layer] = {}
                    # loop over all resources
                    for r in layer_data["Resources"].values():
                        name = r["name"]
                        selected_resources[layer].append(name)
                        # store information about the resources
                        resources_data[layer][name] = {
                            "cost": r["cost"],
                            "description": r["description"],
                            "memory": r["memorySize"]
                        }
                        if "totalNodes" in r.keys():
                            number = r["totalNodes"]
                            resources_data[layer][name]["number"] = number
        return selected_resources, resources_data
    
    @staticmethod
    def is_degraded(common_config_path):
        """
        Check if the application supports components with degraded performance
        """
        filename = "annotations.yaml"
        filepath = os.path.join(common_config_path, filename)
        with open(filepath) as file:
            items = yaml.full_load(file)
        degraded = False
        for item in items:
            if "model_performance" in items[item]:
                degraded = True
                break
        return degraded
    
    @staticmethod
    def parse_component_name(name):
        c = name
        s = ""
        h = ""
        if "partition" in name:
            c = name.split("_partition")[0]
            tokens = re.findall(r'\d+', name)
            s = "X" if len(tokens) == 1 else tokens[0]
            h = tokens[-1]
        return c, s, h
    
    @staticmethod
    def get_alternative_list(application_dir):
        """
        Return the list of alternative deployments sorted by metric value (if 
        this is greater than the user-defined threshold)
        """
        # check if alternative deployments with degraded performance are 
        # available
        common_config_path = os.path.join(application_dir, "common_config")
        degraded = Parser.is_degraded(common_config_path)
        # build list of alternative deployments
        alternative_deployments = ["original_deployment"]
        if degraded:
            # load the performance of all deployments
            space4air_path = os.path.join(application_dir, "space4ai-r")
            filename = "deployments_performance.yaml"
            filepath = os.path.join(space4air_path, filename)
            with open(filepath) as file:
                system = yaml.full_load(file)["System"]
            # get deployments names
            alternative_deployments = []
            for alternative in system["sorted_deployments_performance"]:
                for d, data in alternative.items():
                    if data["metric_value"] >= system["metric_thr"]:
                        alternative_deployments.append(d)
        return alternative_deployments

