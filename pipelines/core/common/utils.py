'''
Created on Nov 15, 2020

@author: sunil.thakur
'''

import importlib


def load_class_by_name(name, package, params=None):
    module = importlib.import_module(package)
    class_ = getattr(module, name)
    if params is None:
        return class_()
    else:
        return class_(params)
    
def call_function_by_name(name, full_class_name):
    package, class_name = full_class_name.rsplit(".",1)
    class_ = load_class_by_name(class_name, package) 
        
    func = getattr(class_, name)
    return func

def has_callable_function(instance, function_name):
    func = getattr(instance, function_name, None)
    return func and callable(func)