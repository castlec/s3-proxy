'''
Created on Oct 7, 2016

@author: christopher.j.castle
'''
import json

from botocore.exceptions import WaiterError
import requests

from request_hooks import objdict
import boto3


class S3JsonHttpProxy(object):
    '''
    classdocs
    '''


    def __init__(self, key_id, access_key):
        '''
        Constructor
        '''

        self.s3 = boto3.resource('s3', 
            aws_access_key_id=key_id, 
            aws_secret_access_key=access_key)
        pass
    
    def run(self, bucket_name, side1, side2):   
        bucket = self.s3.Bucket(bucket_name)
        #read the in
        otherSide = self.s3.Object(bucket_name, side1)
        keepWaiting = True
        while keepWaiting:
            try:
                otherSide.wait_until_exists()
                keepWaiting = False
                print 'Still waiting'
            except WaiterError as we:
                print dir(we)
                print we.last_response
                print we.message
                keepWaiting = 'ObjectExists failed: Forbidden' in we.message
                print keepWaiting
                print 'Weeee!!'
        bytes = otherSide.get()['Body'].read()
        otherSideDict = objdict(json.loads(bytes))
        
        response = requests.request(otherSideDict.method, otherSideDict.url, headers=otherSideDict.headers, body=otherSideDict.body)
        self.write_response(self, response, bucket, side2)
        return response   
    
    def write_response(self, response, bucket, side):   
        headers = {}
        headers.update(response.headers)
        out = {'url':response.url, 'body':response.json(), 'headers':headers}  
        bucket.put_object(Key=side, Body=json.dumps(out))