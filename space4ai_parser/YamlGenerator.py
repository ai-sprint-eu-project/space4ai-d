from Logger import Logger
from Parser import Parser

import yaml
import json
import os


class ParserJsonToYaml(Parser):

    def __init__(
            self, 
            application_dir, 
            who, 
            alternative_deployment=None, 
            log=Logger()
        ):
        """
        Initialize the parser
        """
        super().__init__(application_dir, who, alternative_deployment, log)

    def parse_output_json(self):
        """
        Parse the output json file to retrieve the solution feasibility and 
        the optimal deployment
        """
        # read the output json file
        filename = "Output.json"
        if self.who == "SPACE4AI-D":
            filepath = os.path.join(self.space4aid_path, filename)
        else:
            filepath = os.path.join(self.space4air_path, filename)
        with open(filepath) as file:
            data = json.load(file)
        # check solution feasibility
        feasibility = data["feasible"]
        self.logger.log(
            f"Solution is {'feasible' if feasibility else 'UNFEASIBLE'}"
        )
        # read solution if feasible (or at design-time)
        components_values = []
        if feasibility or self.who == "SPACE4AI-D":
            # loop over all components
            c_keys = data["components"].keys()
            self.logger.log(f"Components keys: {c_keys}", 6)
            for c in c_keys:
                # loop over all deployments
                s_keys = data["components"][c].keys()
                self.logger.log(f"Deployments keys: {s_keys}", 6)
                for s in s_keys:
                    if type(data["components"][c][s]) is dict:
                        # loop over all partitions
                        h_keys = data["components"][c][s].keys()
                        self.logger.log(f"Partitions keys: {h_keys}", 6)
                        for h in h_keys:
                            computational_layer = data["components"][c][s][h]
                            components_values.append(
                                ((c, s, h), computational_layer)
                            )
            self.logger.log(f"Components values: {components_values}", 4)
        return feasibility, components_values

    def find_right_components(self, components_values):
        """
        Build the dictionary of selected components and corresponding 
        deployments
        """
        # load the candidate deployments
        filename = "candidate_deployments.yaml"
        filepath = os.path.join(self.common_config_path, filename)
        with open(filepath) as file:
            deployments = yaml.full_load(file)
        # loop over all components
        components = {}
        for component_value in components_values:
            code = component_value[0]
            component_key = self.code_to_names[code]["key"]
            component_name = self.code_to_names[code]["component"]
            partition_name = self.code_to_names[code]["partition"]
            target_component_name = component_name
            deployment_idx = 1
            # select component key and update component name
            if partition_name != "base":
                _, deployment_idx, partition_idx = self.parse_component_name(
                    partition_name
                )
                component_name += f"_partitionX_{partition_idx}"
                target_component_name += f"_{partition_name}"
            # access component with the given key
            candidate = deployments["Components"][component_key]
            if candidate["name"] == component_name:
                self.logger.log(
                    "Associating code {} to key {} --> {}".\
                        format(code, component_key, target_component_name),
                    3
                )
                components[component_key] = self.update_component(
                    candidate, 
                    component_value[1], 
                    target_component_name,
                    int(deployment_idx) - 1     # list indices start from 0
                )
        return components

    def update_component(self, old_component, layer_info, name, deployment):
        """
        Update the component info gathered from the candidate deployments 
        with the selected resource assignment provided as parameter
        """
        # copies component body from candidate_deployments.yaml
        new_component = old_component
        # fix name
        new_component["name"] = name
        # rename layer key and puts in the correct value
        computational_layer = list(layer_info.keys())[0]
        exec_layer = int(
            computational_layer.replace("computationalLayer", "")
        )
        new_component.pop("candidateExecutionLayers")
        new_component["executionLayer"] = exec_layer
        # pick the correct resource
        chosen_resource = list(layer_info[computational_layer].keys())[0]
        # choose right container
        containers = old_component["Containers"]
        if len(containers.keys()) == 1:
            # if there's only one container, job done
            key = list(containers.keys())[0]
        else:
            # else find the right one
            for key in containers.keys():
                # if there's a match we found our container
                # no error control if no match found, assuming not possible
                if chosen_resource in containers[key]["candidateExecutionResources"]:
                    break
        # select correct container and rename key
        chosen_container = containers[key]
        chosen_container.pop("candidateExecutionResources")
        chosen_container["selectedExecutionResource"] = chosen_resource
        # select correct image and memory size according to the chosen 
        # deployment
        if type(chosen_container["image"]) is list:
            chosen_container["image"] = chosen_container["image"][
                deployment
            ]
            chosen_container["memorySize"] = chosen_container["memorySize"]
        # save correct container in component
        new_component["Containers"] = {"container1": chosen_container}
        return new_component

    def find_right_resources(self, layers):
        """
        """
        # load the candidate resources
        filename = "candidate_resources.yaml"
        filepath = os.path.join(self.common_config_path, filename)
        with open(filepath) as file:
            resources = yaml.full_load(file)["System"]
        net_domains = resources["NetworkDomains"].keys()
        # loop over all the components and associated resources to link 
        # them to the corresponding candidate description
        comp_layer_resource_tuple = []
        for layer in layers:
            computational_layer = list(layer[1].keys())[0]
            resource = list(layer[1][computational_layer].keys())[0]
            assignment = (computational_layer, resource)
            self.logger.log(f"Found assignment {layer[0]} --> {assignment}", 3)
            comp_layer_resource_tuple.append(assignment)
        # loop over network domains to determine which candidate resources 
        # should be removed because they were not selected in the solution
        to_pop = []
        for n in net_domains:
            n_keys = resources["NetworkDomains"][n].keys()
            if "ComputationalLayers" in n_keys:
                domain_data = resources["NetworkDomains"][n]
                computational_layers = domain_data["ComputationalLayers"]
                # loop over all computational layers in the domain
                for cl, cl_data in computational_layers.items():
                    # loop over all resources
                    for r, r_data in cl_data["Resources"].items():
                        resource = r_data["name"]
                        # if not in the solution, pop
                        if (cl, resource) not in comp_layer_resource_tuple:
                            self.logger.log(
                                f"Removing unused resource {[n, cl, r]}",
                                4
                            )
                            to_pop.append([n, cl, r])
                        # otherwise, make sure that it has the correct 
                        # number of nodes
                        else:
                            index = comp_layer_resource_tuple.index(
                                (cl, resource)
                            )
                            solution_data = layers[index][1][cl][resource]
                            if "number" in solution_data.keys():
                                correct_node_number = solution_data["number"]
                                r_data["totalNodes"] = correct_node_number
        # remove unused resources
        for array in to_pop:
            n, cl, r = array
            resources["NetworkDomains"][n]["ComputationalLayers"][cl][
                "Resources"
            ].pop(r)
        # remove unused computational layers & network domains and return
        return self.fix_resources(resources)

    def fix_resources(self, resources):
        """
        Remove unused computational layers and network domains
        """
        # detect layers to remove because they have no more resources, fix 
        # resources number for the others to fill gaps
        to_pop = []
        net_domains = resources["NetworkDomains"].keys()
        for n in net_domains:
            n_keys = resources["NetworkDomains"][n].keys()
            if "ComputationalLayers" in n_keys:
                domain_data = resources["NetworkDomains"][n]
                computational_layers = domain_data["ComputationalLayers"]
                for cl, cl_data in computational_layers.items():
                    layer_resources = cl_data["Resources"]
                    resources_keys = list(layer_resources.keys())
                    if len(resources_keys) == 0:
                        self.logger.log(f"Removing unused layer {[n, cl]}", 4)
                        to_pop.append([n, cl])
                    else:
                        for i in range(len(resources_keys)):
                            if resources_keys[i] != f"resource{i+1}":
                                layer_resources[f"resource{i+1}"] = layer_resources.pop(
                                    resources_keys[i]
                                )
        # remove unused layers
        for array in to_pop:
            n, cl = array
            resources["NetworkDomains"][n]["ComputationalLayers"].pop(cl)
        # detect network domains to be removed because they have no more layers
        to_pop = []
        no_comp_layers_domains = []
        for n, n_data in resources["NetworkDomains"].items():
            n_keys = n_data.keys()
            if "ComputationalLayers" in n_keys:
                computational_layers = n_data["ComputationalLayers"]
                cl_keys = computational_layers.keys()
                if len(cl_keys) == 0:
                    to_pop.append(n)
            else:
                no_comp_layers_domains.append(n)
        # remove unused domains
        for n in to_pop:
            resources["NetworkDomains"].pop(n)
        # at this point all NDs have either valid comp_layers or none;
        # if they have no comp_layers and no valid subdomains, delete domain
        net_domains = set(resources["NetworkDomains"].keys())
        net_domains -= set(no_comp_layers_domains)
        to_pop = []
        for n in no_comp_layers_domains:
            sub_nd = resources["NetworkDomains"][n]["subNetworkDomains"]
            valid_sub_nds = []
            for s in sub_nd:
                if s in net_domains:
                    valid_sub_nds.append(s)
            # if not empty
            if valid_sub_nds:
                resources["NetworkDomains"][n][
                    "subNetworkDomains"
                ] = valid_sub_nds
            else:
                to_pop.append(n)
        for n in to_pop:
            resources["NetworkDomains"].pop(n)
        return resources
    
    def find_deployment_name(self, components):
        """
        Determine which is the name of the chosen deployment
        """
        # load index file
        filename = "multi_cluster_qos_constraints.yaml"
        filepath = os.path.join(
            self.application_dir, 
            "aisprint/deployments", 
            filename
        )
        with open(filepath) as file:
            index = yaml.full_load(file)["System"]["Deployments"]
        # get list of chosen computational layers and components
        chosen_deployment_data = [
            (c["executionLayer"], c["name"]) for c in components.values()
        ]
        # loop over all deployments
        for deployment_name, deployment_data in index.items():
            layers = deployment_data["ExecutionLayers"]
            # check if the chosen deployment corresponds to the current one
            found = True
            for layer, component in chosen_deployment_data:
                if layer not in layers.keys():
                    found = False
                    continue
                else:
                    if component not in layers[layer]["components"]:
                        found = False
                        continue
            # if so, return its name
            if found:
                return deployment_name
        # if no deployment corresponds to the chosen one, there's some error
        self.error.log(
            "The chosen deployment is not found among the available ones"
        )
        return "NOT_FOUND"

    def make_output_yaml(self, feasible, components, resources):
        """
        Write the output yaml file with the optimal production deployment
        """
        # find deployment name
        deployment_name = self.find_deployment_name(components)
        # define output dictionary
        output = {
            "System": {
                "Components": components,
                **resources,
                "Feasible": feasible,
                "DeploymentName": deployment_name
            }
        }
        # define production deployment path
        filename = "production_deployment.yaml"
        filepath = os.path.join(self.optimal_deployment_path, filename)
        # write file
        self.logger.log(f"Writing {filepath}")
        with open(filepath, "w") as file:
            yaml.dump(output, file, sort_keys=False)

    def write_unfeasible_runtime_solution(self):
        """
        If the runtime solution is not feasible, return the current production 
        deployment (with modified feasibility value)
        """
        # load the current production deployment
        filename = "production_deployment.yaml"
        filepath = os.path.join(self.current_deployment_path, filename)
        with open(filepath) as file:
            production_deployment = yaml.full_load(file)
        # change the feasibility
        production_deployment["System"]["Feasible"] = False
        # write to file
        filepath = os.path.join(self.optimal_deployment_path, filename)
        with open(filepath, "w") as file:
            yaml.dump(production_deployment, file, sort_keys=False)

    def main_function(self):
        # extracts useful info from output_json
        self.logger.log("Reading Output.json", 2)
        self.logger.level += 1
        feasible, component_values = self.parse_output_json()
        if feasible or self.who == "SPACE4AI-D":
            # picks the correct components
            final_components = self.find_right_components(component_values)
            # picks the correct resources
            final_resources = self.find_right_resources(component_values)
            self.logger.level -= 1
            # puts them together in the output.yaml file
            self.logger.log("Writing output yaml file", 2)
            self.logger.level += 1
            self.make_output_yaml(feasible, final_components, final_resources)
            self.logger.level -= 1
        # if runtime solution is not feasible, return the current production 
        # deployment (with modified feasibility value)
        else:
            self.logger.level -= 1
            self.logger.log("Copying current production deployment")
            self.write_unfeasible_runtime_solution()
