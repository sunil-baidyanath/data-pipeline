'''
Created on Nov 16, 2020

@author: sunil.thakur
'''
from abc import abstractmethod

class Transformer(object):
    
    def __init__(self, functions=None):
        
        if not functions:
            self.functions = []
            
        self.functions = functions
    
    @abstractmethod
    def transform(self, datasets):
        ''' '''
        
