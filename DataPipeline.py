'''
Created on Nov 19, 2020

@author: sunil.thakur
'''
import os
import sys

from pipelines.core.app import Kernel
from pipelines.core.pipelines import Pipeline

def main(pipeline_name, basedir=None):

    if not basedir:
        basedir = os.path.normpath(os.path.dirname(os.path.abspath(__file__)))
        print(basedir)
    try:    
        app = Kernel.get_instance()    
        app.setup(basedir, pipeline_name)    
         
        pipeline = Pipeline(app.get_pipeline_config())
        pipeline.run()
          
    except:
        raise Exception('Error in application')
    finally:
        app.stop()
    
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        main('sample_local')