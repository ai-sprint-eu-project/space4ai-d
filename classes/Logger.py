#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 21 15:07:51 2021

@author: federicafilippini
"""

import sys


## Logger
#
# Class for logging
class Logger:
    
    ## @var stream
    # Stream for logging (default: sys.stdout)
    
    ## @var verbose
    # Verbosity level (default: 0 - no logging)
    
    ## @var level
    # Indentation level for logging (default: 0 - no indent)
    
    ## @var error
    # True if used to print error messages (default: False)
    
    ## Logger class constructor
    #   @param self The object pointer
    #   @param stream Stream for logging (default: sys.stdout)
    #   @param verbose Verbosity level (default: 0 - no logging)
    #   @param level Indentation level for logging (default: 0 - no indent)
    #   @param error True if used to print error messages (default: False)
    def __init__(self, stream = sys.stdout, verbose = 0, level = 0,
                 error = False):
        self.stream = stream
        self.verbose = verbose
        self.level = level
        self.error = error            
    
    ## Method to support pickling/unpickling of Logger objects
    #   @param self The object pointer
    def __getstate__(self):
        d = self.__dict__.copy()
        if "stream" in d:
            if type(d["stream"]) == "_io.TextIOWrapper":
                l = [d["stream"].name, d["stream"].mode]
            else:
                l = [d["stream"].name]
            d["stream"] = l
        return d

    ## Method to support pickling/unpickling of Logger objects
    #   @param self The object pointer
    #   @param d Dictionary related to the Logger
    def __setstate__(self, d):
        if "stream" in d:
            stream = Logger.parse_wrapper(d["stream"])
            d["stream"] = stream
        self.__dict__.update(d)
    
    ## Method to prepare a string for logging, adding the proper level of 
    # indentation
    #   @param self The object pointer
    #   @param message The string to be printed
    #   @return The new string preceded by the proper number of spaces
    def prepare_logging(self, message):
        full_message = " " * 4 * self.level
        if self.error:
            full_message += "ERROR: "
        full_message += message
        return full_message
    
    ## Method to print the given message
    #   @param message The string to be printed
    #   @param v Minimum verbosity level to print the message
    def log(self, message, v = 0):
        if self.verbose >= v:
            full_message = self.prepare_logging(message)
            print(full_message, file = self.stream)
    
    ## Method to convert a list of stream properties into an actual stream
    #   @param wrapper The list of stream properties. It is defined as 
    #                  [file_name, mode] if the stream is a 
    #                  _io.TextIOWrapper, while it stores [stdout] if the 
    #                  stream is sys.stoud ([sterr], respectively)
    #   @return The corresponding stream
    @staticmethod
    def parse_wrapper(wrapper):
        if len(wrapper) > 1:
            filename = wrapper[0]
            mode = wrapper[1]
            stream = open(filename, mode)
        elif wrapper[0] == "stdout":
            stream = sys.stdout
        else:
            stream = sys.stderr
        return stream
        
