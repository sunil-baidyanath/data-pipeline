'''
Created on Nov 20, 2020

@author: sunil.thakur
'''
import os
import json

from core.data import Dataset
from abc import abstractmethod

class Kernel (object):
    
    
    __instance = None
    
    @staticmethod 
    def get_instance():
        """ Static access method. """
        if Kernel.__instance == None:
            Kernel()
        return Kernel.__instance  
   
    def __init__(self):
        """ Virtually private constructor. """
        if Kernel.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            Kernel.__instance = self

         
    def setup(self, basedir, env_name, pipeline_name):
        print('Setting application root to ', basedir)
        env_config_path = os.path.join(basedir, 'config/envs/')
        with open(os.path.join(env_config_path, env_name+'.json')) as env_config_file:
            env_config = json.load(env_config_file)
            self.context = Context.create(env_config)
        
        pipeline_config_path = os.path.join(basedir, 'config/pipelines/')  
        with open(os.path.join(pipeline_config_path, pipeline_name+'.json')) as pipeline_config_file:
            self.config = json.load(pipeline_config_file)
            
    def get_context(self):
        return self.context
        
    def get_pipeline_config(self):               
        return self.config
        
    def stop(self):
        self.context.stop()    
        
class Context(object):
    
    def __init__(self, config): 
        self.options = config     
    
    @classmethod           
    def create(cls, config):
        if config['compute']['engine']['type'] == 'spark':
            return SparkContext(config['compute']['engine']['options']) 
        
        
#         self.context.conf.set("spark.sql.parquet.cacheMetadata", "false")
#         self.context.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#         self.context.conf.set("spark.sql.parquet.mergeSchema", "true")
#         self.context.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false") 

    @abstractmethod
    def stop(self):
        ''' '''

class SparkContext(Context):
    
    def __init__(self, config):  
              
        from pyspark.sql import SparkSession
        
        self.config = config
        self.spark = SparkSession.builder \
                        .appName('pipeline') \
                        .getOrCreate()
                        
        self.spark.conf.set("spark.executor.memory", "4g")
        self.spark.conf.set("spark.executor.core", "2")
        
    
    def load(self, options):
        if 'csv' == options['dataformat']['type']:
            datapath = options['filepath']
            print('Loading from ', datapath)
            del options['filepath']
            del options['dataformat']
            
            df = self.spark.read.csv(datapath, **options)
            
            return Dataset(df)
    
    def save(self, data, options):
        if 'csv' == options['dataformat']['type']:
            datapath = options['filepath']
            
            del options['filepath']
            del options['dataformat']
            
            data.df.write.csv(datapath, **options)
    
    def stop(self):
        self.spark.stop()