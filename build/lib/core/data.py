'''
Created on Nov 23, 2020

@author: sunil.thakur
'''
class Dataset(object):
    
    def __init__(self, df):
        self.df = df
        self.validators = []
        self.transformers = []
    
    

class SparkDataset(Dataset):
    
    def __init__(self, data):
        super(SparkDataset, self).__init__(data)
        
    