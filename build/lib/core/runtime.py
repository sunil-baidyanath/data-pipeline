'''
Created on Nov 15, 2020

@author: sunil.thakur
'''
import json
from abc import abstractmethod

class Runtime(object):
    
    def __init__(self, config):        
        self.config = config
        
    @abstractmethod
    def start(self, pipeline):
        '''
        '''
    
    @abstractmethod    
    def status(self, jobid):
        '''
        '''


class AWSRuntime(Runtime):
    
    def __init__(self, pipeline_root, env_name):
        self.pipeline_root = pipeline_root
        self.env_name = env_name
        
    def start(self, pipeline_name):
        
        command = f'''
                    aws emr create-cluster 
                        --enable-debugging 
                        --log-uri "s3n://aws-logs-107414700874-us-west-2/elasticmapreduce/" 
                        --name "Pipeline Cluster" --release-label emr-5.8.0 
                        --applications Name=Hadoop Name=Spark 
                        --ec2-attributes KeyName=coretechs-dev-key --instance-type m3.xlarge --instance-count 5 
                        --steps Type=CUSTOM_JAR,Name="Measure Scorer",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=["spark-submit","--deploy-mode","cluster",{self.pipeline_root},"{self.env_name}, {pipeline_name}"] 
                        --auto-terminate --region us-west-2 --use-default-roles
                '''
        
        
        cmd_output = self.run_cmd(command)
        output = json.loads(cmd_output)
        
        return output['ClusterId']


    def status(self, job_id):
        command = f'''
                    aws emr describe-cluster --cluster-id {job_id}
                 '''
        
        output = json.loads(self.run_cmd(command))
        return output['Cluster']['Status']['State']
    