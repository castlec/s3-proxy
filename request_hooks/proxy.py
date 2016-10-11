import argparse
import datetime
import json
import sys

import boto3
from botocore.exceptions import WaiterError
from flask import Flask, request, Response
import requests

from objdict import objdict


app = Flask(__name__)
app.config.from_object(__name__)

app.config.from_envvar('PROXY_SETTINGS')

@app.errorhandler(500)
def error_handling(error):
    print error.message
    print type(error)
    import sys
    import traceback
    print >> sys.stderr, traceback.format_exc()
    return 'ow'

def initS3():
    s3 = boto3.resource('s3', 
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'), 
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY'))
    global bucket_name
    global bucket
    global input_object
    global output_object
    bucket_name = app.config.get('BUCKET_NAME')
    print bucket_name
    bucket = s3.Bucket(bucket_name)
    print 'bucket loaded'
    global input 
    input = app.config.get('IN')
    global output 
    output = app.config.get('OUT')
    print 'input:%s output:%s'% (input, output)
    #this will blow up if the files don't exist?
    input_object = bucket.Object(input)
    # delete the side we're monitoring so we don't handle a stale request
    input_object.delete()
    output_object = bucket.Object(output)


@app.route('/')
def show_entries():
    print type(request)
    print request.__dict__
    print 'path: ' + request.url
    print 'method: ' + request.method
    print request.data
    print 'headers: '
    for header in request.headers:
        print header
        
    headers = {}
    headers.update(request.headers)    
    mapped_request = {'url':request.url, 'method': request.method, 'body':request.data, 'headers':headers}
    print 'forwarding request via s3 file %s'%output_object.key 
    print mapped_request
    output_object.put(Body=json.dumps(mapped_request),  ContentType='text/plain')
    print 'Request forwarded'
        
    return "body"

def _retrieve_s3_input_object():
    print 'Waiting for input on %s' % input_object.key
    keepWaiting = True
    wait_start = datetime.datetime.now()
    while keepWaiting:
        try:
            input_object.wait_until_exists(IfModifiedSince=wait_start)
            keepWaiting = False
            print 'input object received on %s' % input_object.key
        except WaiterError as we:
            keepWaiting = 'ObjectExists failed: Forbidden' in we.message or 'ObjectExists failed: Max attempts exceeded' in we.message 
            if keepWaiting:
                print 'Exceeded max waiter wait. Waiting again.'
            else:
                print 'Unexpected error'
                raise we
    print 'reading from %s' % input_object.key
    bytestring = input_object.get()['Body'].read()
    input_object.delete()
    print bytestring
    print 'deleting %s to stop back and forth' % input_object.key
    otherSideDict = objdict(json.loads(bytestring))
    return otherSideDict


@app.after_request
def handle_s3_response(response):
    otherSideDict = _retrieve_s3_input_object()
    print response.__dict__
    print dir(response)
    newResponse = Response(response=otherSideDict.body, status=int(otherSideDict.status), headers=otherSideDict.headers, mimetype=None, content_type=None)
    return newResponse
    
def stay_a_while_and_listen():
    while True:
        try:
            proxyRequest = _retrieve_s3_input_object()
            print 'Making outbound request on behalf of proxy caller'
            print proxyRequest
            response = requests.request(proxyRequest.method, proxyRequest.url, headers=proxyRequest.headers, data=proxyRequest.body, verify=False)
            headers = {}
            headers.update(response.headers)
            out = {'url':response.url, 'body':response.text, 'headers':headers, 'status':response.status_code} 
            print 'sending object output via %s' % output_object.key 
            print out
            output_object.put(Body=json.dumps(out),  ContentType='text/plain')
        except:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='S3 Proxy')
    parser.add_argument('--mode', help='proxy to start web proxy. receiver to start proxy receiver side.')
    args = parser.parse_args(sys.argv[1:len(sys.argv)])
    initS3()
    if args.mode == 'proxy':
        print 'Starting as proxy'
        app.run()
    elif args.mode == 'receiver':
        print 'Starting as proxy receiver'
        stay_a_while_and_listen()
    else:
        parser.print_help()
