'''
Created on Nov 15, 2020

@author: sunil.thakur
'''
from abc import abstractmethod
import json
import os
import shutil
import subprocess
import boto3
from botocore.exceptions import ClientError

class Runtime(object):
    
    def __init__(self, config=None):
        if config:        
            self.config = config
        else:
            self.config = {}
            self.config['s3_repo'] = {}
            self.config['s3_repo']['bucket'] = 'edh2'
            self.config['s3_repo']['root'] = 'data-pipeline'
            self.config['s3_repo']['lib_path'] = 'lib'
            self.config['s3_repo']['config_path'] = 'config/pipelines'
        
        self.module_path = os.path.dirname(os.path.dirname(__file__))   
        self.root_path = os.path.dirname(self.module_path)
        
        
        
        
    @abstractmethod
    def start(self, pipeline):
        '''
        '''
    
    @abstractmethod    
    def status(self, jobid):
        '''
        '''
    
    def run_cmd(self, cmd):
        output = subprocess.check_output(cmd, shell = True)
        return output
    
    def build(self):
        output_file_path = os.path.join(self.root_path, 'dist/'+os.path.basename(self.module_path)+'.zip')
        
        shutil.make_archive(output_file_path, 'zip', self.module_path)        
        return output_file_path
        
        
class AWSRuntime(Runtime):
    
    def __init__(self, config):
        super(AWSRuntime, self).__init__(config)
        
    def upload_file(self, file_path, bucket, s3_location):  
        s3_client = boto3.client('s3')        
        s3_object = os.path.join(s3_location, os.path.basename(file_path))
        
#         print("Uploading {} to {}".format(file_path, s3_object))
        try:
            s3_client.upload_file(file_path, bucket, s3_object)
        except ClientError:
            return False
        
        return True 
        
    def deploy(self, pipeline_name, build=False):
        options = self.config['s3_repo']
        if build:
            print('Building the project')
            output_file_path = self.build()
            
            # upload module zip file
            print('Deploying the pipeline lib module')
            self.upload_file(output_file_path, options['bucket'], os.path.join(options['root'], options['lib_path']))  
            
            # upload main pipeline module 
            print('Deploying the main pipeline script')         
            self.upload_file('DataPipeline.py', options['bucket'], options['root'])
            
        # upload pipeline config file
        print("Deploying configuration for pipeline '{}'".format(pipeline_name))
        config_path = os.path.join(self.root_path, 'config/pipelines/'+pipeline_name+'.json')
        self.upload_file(config_path, options['bucket'], os.path.join(options['root'], options['config_path']))
        
        print('Pipeline deployed successfully!')
        
    def start(self, pipeline_name):
        
        command = f'''aws emr create-cluster \
                        --release-label emr-5.32.0 \
                        --applications Name=Hadoop Name=Spark \
                        --steps Type=CUSTOM_JAR,ActionOnFailure=CONTINUE,Jar="command-runner.jar",Name="Spark application",Args=["spark-submit","--deploy-mode","cluster","--py-files","s3://edh2/data-pipeline/lib/data-pipeline.zip","--files","s3://edh2/data-pipeline/config/pipelines/{pipeline_name}.json","s3://edh2/data-pipeline/DataPipeline.py","{pipeline_name}"]  \
                        --instance-groups InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m1.medium,Name="Master Instance Group" InstanceCount=1,InstanceGroupType=CORE,InstanceType=m1.large,Name="Core Instance Group"  \
                        --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
                        --auto-terminate  \
                        --enable-debugging \
                        --name "Data Pipeline Cluster" \
                        --log-uri "s3n://aws-logs-859603162954-us-west-2/elasticmapreduce/" \
                        --region us-west-2
                    '''
  
        cmd_output = self.run_cmd(command)
        output = json.loads(cmd_output)
        
        return output['ClusterId']


    def status(self, job_id):
        command = f''' aws emr \
                        describe-cluster --cluster-id {job_id}
                '''
        output = json.loads(self.run_cmd(command)) 
        return output['Cluster']['Status']['State']
    