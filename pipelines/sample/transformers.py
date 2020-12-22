'''
Created on Nov 18, 2020

@author: sunil.thakur
'''
from pipelines.core.transformers import Transformer

class SampleTransformer(Transformer):
    
    def __init__(self, config):
        ''' '''
        
    
    def transform(self, datasets):
#         print(datasets)
        data = datasets['sample_input']
#         print(data)
#         data.df.show(5)
        return {'sample_output': data}