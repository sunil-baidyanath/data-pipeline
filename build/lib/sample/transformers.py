'''
Created on Nov 18, 2020

@author: sunil.thakur
'''
from core.transformers import Transformer

class SampleTransformer(Transformer):
    
    def __init__(self, config):
        ''' '''
        
    
    def transform(self, datasets):
        
        return {'sample_output': datasets['sample_input']}