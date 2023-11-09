from Logger import Logger
from Parser import Parser

import yaml
import json
import sys
import os


class ParserYamlToJson(Parser):

    def __init__(
            self, 
            application_dir, 
            who, 
            alternative_deployment=None, 
            log=Logger(),
            only_edge=False
        ):
        """
        Initialize the parser and load the main yaml files content
        """
        super().__init__(application_dir, who, alternative_deployment, log)
        self.only_edge = only_edge
        self.read_main_yaml_files()
    
    def read_main_yaml_files(self):
        """
        Load the configuration yaml files that are needed for several 
        operations and store the content in variables for later use
        """
        # load the list of components and relative partitions
        filename = "component_partitions.yaml"
        filepath = os.path.join(self.component_partitions_path, filename)
        with open(filepath) as file:
            self.components = yaml.full_load(file)["components"]
        # load the application DAG (i.e., the list of dependencies)
        filename = "application_dag.yaml"
        filepath = os.path.join(self.common_config_path, filename)
        with open(filepath) as file:
            self.dag = yaml.full_load(file)["System"]["dependencies"]
        # load the candidate deployments
        filename = "candidate_deployments.yaml"
        filepath = os.path.join( self.common_config_path, filename)
        with open(filepath) as file:
            self.candidate_deployments = yaml.full_load(file)["Components"]
        # load list of computational layers from SPACE4AI-D yaml file
        filename = "SPACE4AI-D.yaml"
        filepath = os.path.join(self.space4aid_path, filename)
        with open(filepath) as file:
            self.s4aid_config = yaml.full_load(file)
        # load the network domains
        filename = "candidate_resources.yaml"
        filepath = os.path.join(self.common_config_path, filename)
        with open(filepath) as file:
            self.network_domains = yaml.full_load(
                file
            )["System"]["NetworkDomains"]
        # load the QoS constraints
        filename = "qos_constraints.yaml"
        filepath = os.path.join( self.space4aid_path, filename)
        with open(filepath) as file:
            self.constraints = yaml.full_load(file)["system"]
        # loading annotations
        filename = "annotations.yaml"
        filepath = os.path.join(self.common_config_path, filename)
        with open(filepath) as file:
            self.annotations = yaml.full_load(file)
    
    def find_partitions_deployment(self, p1, p2):
        """
        Find the deployment two partitions belong to
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
        # loop over all deployments
        for deployment, deployment_data in index.items():
            # get the list of components in the deployment
            deployment_components = []
            for layer_data in deployment_data["ExecutionLayers"].values():
                deployment_components += layer_data["components"]
            # check if the deployment includes all the given partitions
            if p1 in deployment_components and p2 in deployment_components:
                return deployment
    
    def early_exit_probability(self, p1, p2):
        """
        Return the early exit probability related to the given component 
        partitions
        """
        # find the deployment p1 and p2 belongs to
        deployment = self.find_partitions_deployment(p1, p2)
        self.logger.log(
            f"Partition {p1} with successor {p2} is in {deployment}", 3
        )
        # open the corresponding application_dag file
        filename = "application_dag.yaml"
        filepath = os.path.join(
            self.application_dir, 
            "aisprint/deployments",
            deployment,
            filename
        )
        with open(filepath) as file:
            dag = yaml.full_load(file)["System"]
        for dependency in dag["dependencies"]:
            if dependency[0] == p1 and dependency[1] == p2:
                return 1.0 - dependency[2]
        return 0.0

    def get_components(self):
        """
        Generate the dictionary of components
        """
        # load the size of data produced by each component
        data_size_filename = "components_data_size.yaml"
        filepath = os.path.join(self.oscarp_path, data_size_filename)
        with open(filepath) as file:
            data_sizes = yaml.full_load(file)
        # initialize the components dictionary and the number of components
        components = {}
        number_of_components = len(self.components)
        # cycling on numbers so that I know when I reached the end
        for i in range(number_of_components):
            component_name = list(self.components.keys())[i]
            partitions = self.components[component_name]["partitions"]
            # loop over all partitions
            for p in sorted(partitions):
                c, s, h = self.names_to_code[component_name][p].values()
                # base, partition1_1...
                next_component = [
                    item[1] for item in self.dag if item[0] == component_name
                ]
                next_component_code = []
                if p == "base":
                    components[c] = {}
                    if len(next_component) > 0:
                        for comp in next_component:
                            next_component_code.append(
                                self.names_to_code[comp]["base"]["c"]
                            )
                    eep = 0.0 # no early-exit from base
                    components[c][s] = {
                        h: {
                            "next": next_component_code,
                            "early_exit_probability": eep,
                            "data_size": [data_sizes[component_name]]
                        }
                    }
                else:
                    # splitting "partition1_1" in "partition1_" and "1"
                    partition, n = p.split('_')
                    next_component_part = partition + "_" + str(int(n) + 1)
                    eep = 0.0
                    if next_component_part not in partitions:
                        if len(next_component) > 0:
                            for comp in next_component:
                                next_component_code.append(
                                    self.names_to_code[comp]["base"]["c"]
                                )
                    else:
                        next_component_code.append(
                            self.names_to_code[
                                component_name
                            ][next_component_part]["h"]
                        )
                        eep = self.early_exit_probability(
                            f"{component_name}_{p}",
                            f"{component_name}_{next_component_part}"
                        )
                    if s not in components[c].keys():
                        components[c][s] = {}
                    next_component_name = component_name + "_" + p
                    components[c][s][h] = {
                        "next": next_component_code,
                        "early_exit_probability": eep,
                        "data_size": [data_sizes[next_component_name]]
                    }
        return components
    
    def get_degraded_deployment(self, target_deployment_name):
        """
        Get the list of components and relative dependencies for a specific 
        deployment when alternative with degraded performance are available
        """
        # load the performance of all deployments
        filename = "deployments_performance.yaml"
        filepath = os.path.join(self.space4air_path, filename)
        with open(filepath) as file:
            system = yaml.full_load(file)["System"]
        # extract information about the required deployment
        sorted_deployments = system["sorted_deployments_performance"]
        for deployment in sorted_deployments:
            d_name = list(deployment.keys())[0]
            d_data = deployment[d_name]
            if d_name == target_deployment_name:
                components = d_data["components"]
                dependencies = d_data["dependencies"]
                return components, dependencies
     
    def get_components_details(self, name, resource):
        """
        Get information (memory requirements) about the given component when 
        executed on the given resource
        """
        # generate the list of docker images and relative characteristics
        # loop over all candidate components
        containers = {}
        target_name, deployment, partition_number_target = \
            self.parse_component_name(name)
        for c in self.candidate_deployments:
            component = self.candidate_deployments[c]
            # for non-partitioned components, all the additional information 
            # are in the Containers section (we are interested in the memory 
            # size only)
            if "partition" not in name:
                if component["name"] == name:
                    containers = component["Containers"]
                    break
            else:
                # if the current element is a component partition, check its 
                # name and number
                if "partition" in component["name"]:
                    component_name, _, partition_number_component = \
                        self.parse_component_name(component["name"])
                    # if the current partition is the target one, save the 
                    # corresponding information
                    if partition_number_target == partition_number_component:
                        if target_name == component_name:
                            containers = component["Containers"]
        # loop over all containers, check which corresponds to the given 
        # resource and return the memory requirement for the target deployment
        for _, container in containers.items():
            if resource in container["candidateExecutionResources"]:
                memory = container["memorySize"]
                if type(memory) is list:
                    return memory[int(deployment) - 1]
                else:
                    return memory
        return None
      
    def get_resources(self):
        """
        Load candidate resources
        """
        # initialize resources dictionary
        resources = {}
        resources_types = ["EdgeResources"]
        if not self.only_edge:
            resources_types += ["CloudResources", "FaaSResources"]
        # loop over all resource types
        for resources_type in resources_types:
            if resources_type in self.s4aid_config.keys():
                resources[resources_type] = {}
                layers = self.s4aid_config[resources_type]
                # loop over all computational layers of the current type
                for layer_name in layers:
                    resources[resources_type][layer_name] = {}
                    layer = self.get_layer(layer_name)
                    # loop over all resources in the current layer
                    for resource in layer["Resources"]:
                        resource = layer["Resources"][resource]
                        name = resource["name"]
                        if "faas" in name.lower():
                            # get the transitionCost of last FaaS as general 
                            # transitionCost for json
                            resources[resources_type][layer_name][
                                "transition_cost"
                            ] = resource["transitionCost"]
                            # get the resource characteristics
                            resources[resources_type][layer_name][name] = {
                                "description": resource["description"],
                                "cost": resource["cost"],
                                "memory": resource["memorySize"],
                                "idle_time_before_kill": resource["idleTime"]
                            }
                        else:
                            # get the maximum number of instances for the 
                            # current resource
                            n_cores = resource["processors"]["processor1"][
                                "computingUnits"
                            ]
                            # get the resource characteristics
                            resources[resources_type][layer_name][name] = {
                                "description": resource["description"],
                                "number": resource["totalNodes"],
                                "cost": resource["cost"],
                                "memory": resource["memorySize"],
                                "n_cores": n_cores
                            }
        return resources
      
    def get_layer(self, target_layer):
        """
        Get the description of the candidate resources in the given 
        computational layer
        """
        # loop over all network domains
        for domain_id in self.network_domains.keys():
            layers = self.network_domains[domain_id][
                "ComputationalLayers"
            ].keys()
            # loop over all computational layers in the current domain
            for layer in layers:
                # if the current layer is the target one, return the 
                # corresponding information
                if layer.lower() == target_layer.lower():
                    return self.network_domains[domain_id][
                        "ComputationalLayers"
                    ][layer]
        return None
       
    def get_network(self):
        """
        Get the description of the network domains and their characteristics
        """
        # loop over all network domains
        network_tech = {}
        for domain_id in self.network_domains.keys():
            # get the network characteristics and the list of computational 
            # layers in the current domain
            access_delay = self.network_domains[domain_id]["AccessDelay"]
            bandwidth = self.network_domains[domain_id]["Bandwidth"]
            layers = self.get_domain_layers(domain_id)
            network_tech[domain_id] = {
                "computationalLayers": layers,
                "AccessDelay": access_delay,
                "Bandwidth": bandwidth
            }
        return network_tech
      
    def get_domain_layers(self, target_domain_id):
        """
        Get the list of computational layers in the given network domain
        """
        # loop over all network domains
        layers = []
        for domain_id in self.network_domains.keys():
            # if the current domain is the target one, generate the list of 
            # computational layers included in it
            if target_domain_id == domain_id:
                network_domain = self.network_domains[domain_id]
                if "ComputationalLayers" in network_domain:
                    layers += list(network_domain["ComputationalLayers"].keys())
                # add the layers in all subdomains (if any)
                for subdomain_id in network_domain["subNetworkDomains"]:
                    layers += self.get_domain_layers(subdomain_id)
        return layers
       
    def get_compatibility_matrix(self):
        """
        Generate the dictionary corresponding to the compatibility matrix
        """
        # loop over all components
        compatibility_matrix = {}
        for component in self.components:
            # loop over all partitions
            for partition in sorted(self.components[component]["partitions"]):
                # load the candidate resources for each partition
                if partition == "base":
                    resources = self.get_component_resources(component, "")
                    component_name = component
                else:
                    resources = self.get_component_resources(
                        component, 
                        partition
                    )
                    component_name = component + "_" + partition
                # loop over all candidate resources
                for resource in resources:
                    # get the memory requirement and the component and 
                    # partition names
                    memory = self.get_components_details(
                        component_name, 
                        resource
                    )
                    c, _, h = self.names_to_code[component][partition].values()
                    # add the info to the compatibility dictionary
                    if c not in compatibility_matrix.keys():
                        compatibility_matrix[c] = {}
                    if h not in compatibility_matrix[c].keys():
                        compatibility_matrix[c][h] = []
                    compatibility_matrix[c][h].append({
                        "resource": resource,
                        "memory": memory
                    })
        self.logger.log(f"compatibility_matrix = {compatibility_matrix}", 5)
        return compatibility_matrix 
    
    def get_component_resources(self, target_component_name, target_partition):
        """
        Get the candidate resources for the given component partition
        """
        # loop over all components
        for component_name in self.candidate_deployments.keys():
            resources = []
            component = self.candidate_deployments[component_name]
            # if a target partition is specified (i.e., the target partition 
            # is not 'base')
            if target_partition != "":
                if "partition" in component["name"]:
                    # get the current partition name
                    _, partition_group_target, partition_number_target = \
                        self.parse_component_name(target_partition)
                    name, _, partition_number_component = \
                        self.parse_component_name(component["name"])
                    self.logger.log(
                        "Target partition: {}; group={}, number={}".\
                            format(
                                target_component_name, 
                                partition_group_target, 
                                partition_number_target
                            ), 
                        5
                    )
                    self.logger.log(
                        "Current partition: {}; group={}, number={}".\
                            format(
                                name, 
                                " ", 
                                partition_number_component
                            ), 
                        5
                    )
                    # if the current partition is the target one
                    if target_component_name == name and \
                        partition_number_target == partition_number_component:
                        # load the candidate resources
                        containers = component["Containers"]
                        for container_name in containers:
                            container = containers[container_name]
                            for r in container["candidateExecutionResources"]:
                                if r not in resources:
                                    resources.append(r)
                        return resources
            # else, the target partition is 'base'
            else:
                # if the current component is the target one
                if component["name"] == target_component_name:
                    # load the candidate resources
                    containers = component["Containers"]
                    for container_name in containers:
                        container = containers[container_name]
                        for r in container["candidateExecutionResources"]:
                            if r not in resources:
                                resources.append(r)
                    return resources
    
    def find_alternative_component(self, c):
        """
        Return the key corresponding to the selected alternative for the 
        given component
        """
        for alternative_name in self.components.keys():
            alternative_c, _, _ = self.names_to_code[
                alternative_name
            ]["base"].values()
            if alternative_c.split("_")[0] == c:
                return alternative_c
    
    def get_local_constraints(self, application_components):
        """
        Get the local constraints
        """
        constraints = self.constraints["local_constraints"]
        local_constraints = {}
        # loop over all constraints
        for constraint in constraints:
            name = constraints[constraint]["component_name"]
            threshold = constraints[constraint]["threshold"]
            c, _, _ = self.names_to_code[name]["base"].values()
            # if we are considering a degraded alternative deployment, we 
            # may have to adapt the constraint definition finding the 
            # corresponding alternative component
            if self.degraded and \
                self.alternative_deployment != "original_deployment":
                if name not in self.components.keys():
                    c = self.find_alternative_component(c)
            # check if the component is included in the dictionary of 
            # application components
            if c in application_components:
                # add constraint
                local_constraints[c] = {"local_res_time": threshold}
        return local_constraints
    
    def get_global_constraints(self, application_components):
        """
        Get the global constraints
        """
        constraints = self.constraints["global_constraints"]
        global_constraints = {}
        # loop over all constraints
        for constraint in constraints:
            path_components = constraints[constraint]["path_components"]
            threshold = constraints[constraint]["threshold"]
            components = []
            for name in path_components:
                c, _, _ = self.names_to_code[name]["base"].values()
                # if we are considering a degraded alternative deployment, 
                # we may have to adapt the constraint definition finding 
                # the corresponding alternative component
                if self.degraded and \
                    self.alternative_deployment != "original_deployment":
                    if name not in self.components.keys():
                        c = self.find_alternative_component(c)
                # check if the component is included in the dictionary of 
                # application components
                if c in application_components:
                    components.append(c)
            # add constraint
            global_constraints[constraint] = {
                "components": components,
                "global_res_time": threshold
            }
        return global_constraints
    
    def get_dag(self, application_components):
        """
        Get the application DAG
        """
        # loop over all dependency relations listed in the application dag
        dag = {}
        for dependency in self.dag:
            # get the component names
            component_a = dependency[0]
            component_b = dependency[1]
            component_a,_,_ = self.names_to_code[component_a]["base"].values()
            component_b,_,_ = self.names_to_code[component_b]["base"].values()
            # check if the components are included in the dictionary of 
            # application components
            if component_a in application_components and \
                component_b in application_components:
                # get the transition probability
                transition_probability = dependency[2]
                # add the components and the dependency relation to the dag
                if component_a not in dag.keys():
                    dag[component_a] = {
                        "next": [component_b],
                        "transition_probability": [transition_probability],
                    }
                else:
                    dag[component_a]["next"].append(component_b)
                    dag[component_a]["transition_probability"].append(
                        transition_probability
                    )
        # add terminal nodes to the dag
        if len(dag) < len(application_components):
            self.logger.level += 1
            self.logger.log("Adding terminal nodes to the DAG", 3)
            for component_name in application_components:
                if component_name not in dag:
                    dag[component_name] = {"next": []}
            self.logger.level -= 1
        return dag
        
    def get_performance_models(self):
        """
        Get the performance-related information (name of the performance model 
        to be considered, path to the regressor file, average execution time 
        of the single job, etc.)
        """
        # load json file
        filename = "performance_models.json"
        filepath = os.path.join(self.oscarp_path, filename)
        with open(filepath) as file:
            data = json.load(file)
        # loop over all components
        performance = {}
        for component_name, component in data.items():
            # consider only relevant components (check required in the case 
            # of degraded-performance deployments)
            if component_name in self.components.keys():
                # loop over all component partitions
                for partition_name, partition in component.items():
                    self.logger.log(
                        f"c={component_name}, h={partition_name}", 
                        3
                    )
                    # get the component and partition names
                    if component_name == partition_name:
                        c, _, h = self.names_to_code[
                            component_name
                        ]["base"].values()
                        # if considering the base partition, add a new item to 
                        # the performance dictionary
                        performance[c] = {}
                    else:
                        partition_name = partition_name.strip(
                            component_name + "_"
                        )
                        c, _, h = self.names_to_code[
                            component_name
                        ][partition_name].values()
                    # add the information to the dictionary
                    performance[c][h] = partition
                    # update the path to the regressor files (if any)
                    for j in performance[c][h]:
                        if "regressor_file" in performance[c][h][j]:
                            rf = os.path.join(
                                self.oscarp_path,
                                performance[c][h][j]["regressor_file"]
                            )
                            performance[c][h][j]["regressor_file"] = rf
        return performance
    
    def get_selected_components(self, production_deployment):
        """
        Return the list of selected components from the production deployment
        """
        # loop over all components
        selected_components = {}
        for component in production_deployment["Components"]:
            component_data = production_deployment["Components"][component]
            # get the deployment and partition name
            if "partition" not in component:
                name = component_data["name"]
                c, s, h = self.names_to_code[name]["base"].values()
            else:
                name, dep_id, part_id = self.parse_component_name(
                    component_data["name"]
                )
                part_name = f"partition{dep_id}_{part_id}"
                c, s, h = self.names_to_code[name][part_name].values()
            # get the information about the resource where h runs
            container = component_data["Containers"]["container1"]
            resource = container["selectedExecutionResource"]
            layer = f"computationalLayer{component_data['executionLayer']}"
            # add component to the dictionary
            if c not in selected_components.keys():
                selected_components[c] = {}
            if s not in selected_components[c].keys():
                selected_components[c][s] = {}
            self.logger.log(
                f"Found ({c},{s},{h}) in production deployment", 5
            )
            selected_components[c][s][h] = {layer: {resource: {}}}
        return selected_components
    
    def get_production_deployment(self):
        """
        Load the production deployment file and return the dictionaries of 
        selected components and resources
        """
        # load the production deployment
        filename = "production_deployment.yaml"
        filepath = os.path.join(self.current_deployment_path, filename)
        with open(filepath) as file:
            production_deployment = yaml.full_load(file)["System"]
        # get the selected components and partitions
        selected_components = self.get_selected_components(
            production_deployment
        )
        # get the selected resources
        selected_resources, resources_data = self.get_selected_resources(
            production_deployment
        )
        return selected_components, selected_resources, resources_data

    def get_design_time_solution(self):
        """
        Load the design-time solution file and return the dictionaries of 
        selected components and resources
        """
        # load the production deployment
        filename = "Output.json"
        filepath = os.path.join(self.space4aid_path, filename)
        with open(filepath) as file:
            design_time_solution = yaml.full_load(file)
        # get the selected components and partitions and the selected resources
        selected_components = {}
        selected_resources = {}
        resources_data = {}
        # loop over all components
        for c, component_data in design_time_solution["components"].items():
            for key, deployment_data in component_data.items():
                if key.startswith("response_time"):
                    continue
                s = key
                for h, partition_data in deployment_data.items():
                    # add component to the dictionary
                    if c not in selected_components.keys():
                        selected_components[c] = {}
                    if s not in selected_components[c].keys():
                        selected_components[c][s] = {}
                    self.logger.log(
                        f"Found ({c},{s},{h}) in design-time solution", 5
                    )
                    # loop over all computational layers and get the list of 
                    # corresponding resources
                    for key, layer_data in partition_data.items():
                        if key.startswith("response_time"):
                            continue
                        layer = key
                        selected_resources[layer] = []
                        resources_data[layer] = {}
                        # loop over all resources
                        for resource, resource_data in layer_data.items():
                            selected_resources[layer].append(resource)
                            # store information about the resources
                            resources_data[layer][resource] = resource_data
                            # store information about the component 
                            # partition
                            selected_components[c][s][h] = {
                                layer: {resource: {}}
                            }
        return selected_components, selected_resources, resources_data

    def get_runtime_resources(self):
        """
        Get the resources that should be considered at runtime (i.e., the 
        ones selected in the current production deployment and all the 
        FaaS configurations)
        """
        # get the selected resources in the production deployment
        _, selected_resources, resource_data = self.get_design_time_solution()
        # get all the candidate resources
        all_resources = self.get_resources()
        # add to the candidate resources those that were selected 
        # in the production deployment
        candidate_res = {}
        resource_intersection = {}
        for layer, resources in selected_resources.items():
            resource_type = ""
            if "EdgeResources" in all_resources.keys() and \
                layer in all_resources["EdgeResources"].keys():
                resource_type = "EdgeResources"
            elif "CloudResources" in all_resources.keys() and \
                layer in all_resources["CloudResources"].keys():
                resource_type = "CloudResources"
            if resource_type != "":
                if resource_type not in candidate_res.keys():
                    candidate_res[resource_type] = {}
                candidate_res[resource_type][layer] = {}
                resource_intersection[layer] = []
                for resource in resources:
                    res = all_resources[resource_type][layer][resource]
                    dt_res = resource_data[layer][resource]
                    # check instance number (must be what was selected at 
                    # design time)
                    if "number" in res:
                        res["number"] = dt_res["number"]
                    candidate_res[resource_type][layer][resource] = res
                    resource_intersection[layer].append(resource)
        # the runtime candidate resources include all the FaaS resources
        if "FaaSResources" in all_resources.keys():
            candidate_res["FaaSResources"] = all_resources["FaaSResources"]
        return candidate_res, resource_intersection
    
    def get_expected_throughput(self):
        """
        Get the expected throughput from the annotations
        """
        for component_data in self.annotations.values():
            component_name = component_data["component_name"]["name"]
            if component_name in self.names_to_code.keys():
                if self.names_to_code[component_name]["base"]["c"] == "c1":
                    if "expected_throughput" in component_data.keys():
                        return component_data["expected_throughput"]["rate"]
        return -1
    
    def filter_components(
            self, 
            all_components, 
            full_compatibility_matrix, 
            all_performance_models, 
            selected_resources,
            candidate_faas_resources
        ):
        """
        Filter from the dictionary of all components, the compatibility matrix 
        and the performance models the elements that cannot be selected 
        because the corresponding resources are not in the dictionary 
        of candidates
        """
        components = {}
        compatibility_matrix = {}
        performance_models = {}
        # loop over the compatibility matrix
        items_to_add = {}
        for c, component_data in full_compatibility_matrix.items():
            # loop over all partitions
            for h, partition_data in component_data.items():
                # loop over all resources
                for resource_data in partition_data:
                    resource = resource_data["resource"]
                    to_add = False
                    # check if it belongs to the selected resources
                    to_add = ([resource] in selected_resources.values())
                    # otherwise, check if it is a FaaS resource
                    if not to_add:
                        if candidate_faas_resources is not None:
                            # remember: all FaaS resources are in the same 
                            # computational layer
                            faas_keys = list(
                                candidate_faas_resources.values()
                            )[0].keys()
                            to_add = (resource in faas_keys)
                    if to_add:
                        # register among items to add
                        if c not in items_to_add.keys():
                            items_to_add[c] = {}
                        if h not in items_to_add[c].keys():
                            items_to_add[c][h] = {
                                "compatibility": [],
                                "performance": {}
                            }
                        items_to_add[c][h]["compatibility"].append(
                            resource_data
                        )
                        items_to_add[c][h]["performance"][
                            resource
                        ] = all_performance_models[c][h][resource]
        # loop over all components
        components_to_drop = []
        for c, component_data in all_components.items():
            if c not in items_to_add.keys():
                self.logger.warn(f"No candidate resources for component {c}")
                components_to_drop.append(c)
            else:
                partitions_to_add = set(items_to_add[c].keys())
                # loop over all deployments
                for s, deployment_data in component_data.items():
                    partitions = set(deployment_data.keys())
                    # if all partitions are among the items to add, proceed
                    if partitions.intersection(partitions_to_add) == partitions:
                        # add the deployment to the dictionary of components
                        if c not in components.keys():
                            components[c] = {}
                        components[c][s] = deployment_data
                        # loop over the deployment partitions
                        for h in partitions:
                            item_to_add = items_to_add[c][h]
                            # add to compatibility matrix
                            if c not in compatibility_matrix.keys():
                                compatibility_matrix[c] = {}
                            if h not in compatibility_matrix[c].keys():
                                compatibility_matrix[c][h] = []
                            compatibility_matrix[c][h] = item_to_add[
                                "compatibility"
                            ]
                            # add to performance models
                            if c not in performance_models.keys():
                                performance_models[c] = {}
                            if h not in performance_models[c].keys():
                                performance_models[c][h] = {}
                            performance_models[c][h] = item_to_add[
                                "performance"
                            ]
        # if there are components with no candidate resources, remove them 
        # from the "next" field in other components
        for component in components_to_drop:
            # look for predecessors
            for c in components:
                for d in components[c]:
                    for h in components[c][d]:
                        if component in components[c][d][h]["next"]:
                            components[c][d][h]["next"].remove(component)
        return components, compatibility_matrix, performance_models
    
    def filter_network_domains(self, all_network_domains, candidate_resources):
        """
        Filter only the network domains that include candidate resources
        """
        # list candidate computational layers
        candidate_layers = []
        for layers in candidate_resources.values():
            candidate_layers += layers.keys()
        # loop over all network domains
        network_domains = {}
        no_layers_nd = []
        for network_domain, network_domain_data in all_network_domains.items():
            # check if the network domain includes computational layers
            if "computationalLayers" in network_domain_data.keys():
                layers = network_domain_data["computationalLayers"]
                # loop over all layers
                for layer in layers:
                    # if the layer is included in the candidates, add it to 
                    # the candidate network domains
                    if layer in candidate_layers:
                        if network_domain not in network_domains.keys():
                            network_domains[network_domain] = {}
                            network_domains[network_domain][
                                "computationalLayers"
                            ] = []
                            network_domains[network_domain][
                                "AccessDelay"
                            ] = network_domain_data["AccessDelay"]
                            network_domains[network_domain][
                                "Bandwidth"
                            ] = network_domain_data["Bandwidth"]
                        network_domains[network_domain][
                            "computationalLayers"
                        ].append(layer)
            else:
                # if the domain does not include computational layers, add it 
                # to the list that should be processed later: subdomains can 
                # be added to the candidate only if they were previously 
                # included due to their computational layers
                no_layers_nd.append(network_domain)
        # check domains with no computational layers
        for network_domain in no_layers_nd:
            network_domain_data = all_network_domains[network_domain]
            subNets = network_domain_data["subNetworkDomains"]
            for subNet in subNets:
                # if the sub-network-domain is among the candidates, add it
                if subNet in network_domains.keys():
                    if network_domain not in network_domains.keys():
                        network_domains[network_domain] = {}
                        network_domains[network_domain][
                            "subNetworkDomains"
                        ] = []
                        network_domains[network_domain][
                            "AccessDelay"
                        ] = network_domain_data["AccessDelay"]
                        network_domains[network_domain][
                            "Bandwidth"
                        ] = network_domain_data["Bandwidth"]
                    network_domains[network_domain][
                        "subNetworkDomains"
                    ].append(subNet)
        return network_domains
    
    def make_runtime_system_file(self):
        """
        Generate json file with system description for the runtime
        """
        system_file = {}
        # resources
        self.logger.log("Loading resources")
        self.logger.level += 1
        candidate_resources, res_intersection = self.get_runtime_resources()
        system_file = candidate_resources
        self.logger.level -= 1
        # components, compatibility matrix and performance models
        self.logger.log(
            "Loading components, compatibility matrix and performance models"
        )
        self.logger.level += 1
        components, compatibility_matrix, models = self.filter_components(
            self.get_components(),
            self.get_compatibility_matrix(),
            self.get_performance_models(),
            res_intersection,
            candidate_resources.get("FaaSResources")
        )
        system_file["Components"] = components
        system_file["CompatibilityMatrix"] = compatibility_matrix
        system_file["Performance"] = models
        self.logger.level -= 1
        # network domains
        self.logger.log("Loading network domains")
        self.logger.level += 1
        system_file["NetworkTechnology"] = self.filter_network_domains(
            self.get_network(),
            candidate_resources
        )
        self.logger.level -= 1
        return system_file
    
    def make_design_system_file(self):
        """
        Generate json file with system description at design time
        """
        # resources
        self.logger.log("Loading resources")
        self.logger.level += 1
        system_file = self.get_resources()
        self.logger.level -= 1
        # components
        self.logger.log("Loading components")
        self.logger.level += 1
        system_file["Components"] = self.get_components()
        self.logger.level -= 1
        # compatibility matrix
        self.logger.log("Loading compatibility matrix")
        self.logger.level += 1
        system_file["CompatibilityMatrix"] = self.get_compatibility_matrix()
        self.logger.level -= 1
        # performance dictionary
        self.logger.log("Loading performance dictionary")
        self.logger.level += 1
        system_file["Performance"] = self.get_performance_models()
        self.logger.level -= 1
        # network domains
        self.logger.log("Loading network domains")
        self.logger.level += 1
        system_file["NetworkTechnology"] = self.get_network()
        self.logger.level -= 1
        return system_file
    
    def update_components_and_dag(self):
        """
        Update the dictionary of components and the application dag according 
        to the required alternative deployment
        """
        component_names, dependencies = self.get_degraded_deployment(
            self.alternative_deployment
        )
        self.components = {
            c: {"partitions": ["base"]} for c in component_names
        }
        self.dag = dependencies

    def make_system_file(self):
        """
        Generate json file with system description
        """
        system_file = {}
        # when deployments with degraded performance are considered, we 
        # extract the list of components and dependencies in the target 
        # alternative deployment and update the application DAG
        if self.degraded:
            self.update_components_and_dag()
        # load initial system description
        degraded_design = False
        if self.who == "SPACE4AI-D" and self.degraded:
            if self.alternative_deployment != "original_deployment":
                degraded_design = True
                self.logger.warn(
                    "Generating input starting from design-time solution " +\
                        "in space4ai-d/Output.json. Expect a failure if it " +\
                            "does not exist or has the wrong format"
                )
        if self.who == "SPACE4AI-R" or degraded_design:
            system_file = self.make_runtime_system_file()
        else:
            system_file = self.make_design_system_file()
        # check if the system has available resources
        if "EdgeResources" not in system_file and \
            "CloudResources" not in system_file and \
                "FaaSResources" not in system_file:
            self.logger.warn(
                "No candidate resources; the system cannot be optimized"
            )
            system_file["LocalConstraints"] = {}
            system_file["GlobalConstraints"] = {}
            system_file["DirectedAcyclicGraph"] = {}
            system_file["Lambda"] = {}
            system_file["Time"] = {}
        else:
            # constraints and application dag
            self.logger.log("Loading local constraints")
            system_file["LocalConstraints"] = self.get_local_constraints(
                system_file["Components"]
            )
            self.logger.log("Loading global constraints")
            system_file["GlobalConstraints"] = self.get_global_constraints(
                system_file["Components"]
            )
            self.logger.log("Loading DAG")
            system_file["DirectedAcyclicGraph"] = self.get_dag(
                system_file["Components"]
            )
            # expected throughput
            self.logger.log("Getting Lambda from annotations")
            system_file["Lambda"] = self.get_expected_throughput()
            if system_file["Lambda"] <= 0.0:
                self.error.log("Missing or invalid expected throughput.")
                sys.exit(1)
            # execution time
            #self.logger.log("Time = 1 (hardcoded)")
            system_file["Time"] = self.s4aid_config["Time"]
        # write system description to a json file
        filename = "SystemFile.json"
        if self.who == "SPACE4AI-R":
            filepath = os.path.join(self.space4air_path, filename)
        else:
            filepath = os.path.join( self.space4aid_path, filename)
        with open(filepath, "w") as file:
            json.dump(system_file, file, indent=4)
        return filepath

    def make_input_json(self):
        """
        Generate the input json file with the configuration parameters for 
        space4ai-d (e.g., methods to use, seed, etc)
        """
        methods = self.s4aid_config["Methods"]
        seed = self.s4aid_config.get("Seed", 1)
        verbose_level = self.s4aid_config.get("VerboseLevel", 0)
        # write json file
        filename = "Input.json"
        filepath = os.path.join(self.space4aid_path, filename)
        with open(filepath, "w") as file:
            json.dump({
                    "Methods": methods, 
                    "Seed": seed, 
                    "VerboseLevel": verbose_level
                }, file, indent=4
            )
        return filepath
    
    def make_current_solution(self):
        """
        Generate the current solution file starting from the production 
        deployment
        """
        # read current production deployment
        components, _, resources_data = self.get_production_deployment()
        # update the component description
        for component_data in components.values():
            for deployment_data in component_data.values():
                for partition_data in deployment_data.values():
                    for layer, layer_data in partition_data.items():
                        for res in layer_data.keys():
                            layer_data[res] = resources_data[layer][res]
        # update the component names selecting the corresponding alternatives 
        # in the case of a degraded-performance deployment
        if self.degraded:
            updated_components = {}
            self.update_components_and_dag()
            for c in components.keys():
                name = self.code_to_names[(c, "s1", "h1")]["component"]
                if name not in self.components:
                    alternative_c = self.find_alternative_component(c)
                    updated_components[alternative_c] = components[c]
                else:
                    updated_components[c] = components[c]
            components = updated_components
        # define current solution
        current_solution = {
            "components": components,
            "global_constraints": None,
            "total_cost": None
        }
        # write to a file
        filename = "CurrentSolution.json"
        filepath = os.path.join(self.space4air_path, filename)
        with open(filepath, "w") as file:
            json.dump(current_solution, file, indent=4)
        return filepath
    
    def adjust_resources_number(self, deployment_data, edge_resources):
        """
        Set the number of instances for selected edge resources to the 
        number specified in the given deployment
        """
        # loop over all partitions in the deployment
        for h, assignment in deployment_data.items():
            for layer, layer_data in assignment.items():
                # check if the partition is executed on an edge resource; if 
                # so, update the resource number in the system description
                if layer in edge_resources:
                    # remember: a unique resource is selected in each edge layer
                    resource = list(layer_data.keys())[0]
                    number = layer_data[resource]["number"]
                    edge_resources[layer][resource]["number"] = number
                    self.logger.log(
                        f"Resource {resource} assigned to {h} is " +\
                        f"updated with number {number}", 
                        2
                    )
        return edge_resources

    def filter_current_solution(self):
        """
        Filter content of current solution for only-edge runtime scenarios
        """
        filename = "CurrentSolution.json"
        filepath = os.path.join(self.space4air_path, filename)
        feasible_solution = True
        optimizable_system = True
        if self.who == "SPACE4AI-R" and self.only_edge:
            self.logger.log(
                "Filtering unavailable components from current solution"
            )
            self.logger.level += 1
            # load the current solution
            with open(filepath, "r") as file:
                current_solution = json.load(file)
            # load the available components from the system description
            filename = "SystemFile.json"
            system_filepath = os.path.join(self.space4air_path, filename)
            with open(system_filepath, "r") as file:
                system = json.load(file)
            available_components = system["Components"]
            # check if there are edge resources in the system; exit otherwise
            if "EdgeResources" not in system:
                self.logger.warn(
                  "No candidate edge resources; the system cannot be optimized"
                )
                feasible_solution = False
                optimizable_system = False
            else:
                # check the current solution to remove unavailable components
                unfeasible = {}
                for c,component_data in current_solution["components"].items():
                    if c in available_components:
                        # loop over all deployments
                        for s, deployment_data in component_data.items():
                            if s not in available_components[c]:
                                # if not available, add to the dictionary of 
                                # components to be replaced
                                self.logger.log(
                                    f"Component ({c},{s}) has to be replaced", 
                                    2
                                )
                                unfeasible[c] = s
                                feasible_solution = False
                    else:
                        self.logger.log(f"Component {c} has to be removed", 2)
                        unfeasible[c] = list(component_data.keys())[0]
                        feasible_solution = False
                self.logger.level -= 1
            # if the current solution had unfeasible assignments...
            if not feasible_solution:
                # ...update the current solution
                current_solution["feasible"] = False
                with open(filepath, "w") as file:
                    json.dump(current_solution, file, indent=4)
        return filepath, feasible_solution, optimizable_system
