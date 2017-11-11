import requests
import sys
import os
import os.path
import subprocess
from subprocess import call

lambda_instances = 10
replication = 1
global_bucket = "s3://shelby-lambda-in"

def generate_payload(script_string,bytes_s, bytes_e, index):
    payload=" '{\"code\":" + "\"" + script_string + "\"" + "," + "\"globals\":" + \
    "{\"StartOffset\":" + str(bytes_s)+ "," + "\"EndOffset\":" + str(bytes_e) +\
    "," + "\"InstanceIndex\":" + str(index) + "}}'"
    return payload

def g_generate_payload(script_string,bytes_s, bytes_e, index):
    payload=" --payload \'{\"InstanceIndex\":" + str(index) + "," + "\"StartOffset\":" + \
    str(bytes_s)+ "," + "\"EndOffset\":" + str(bytes_e)+"}\' "
    print payload
    return payload


# EndOffset
# InstanceIndex


def main():
    if (len(sys.argv) < 3):
        raise ValueError("Not enough arguments")

    script_file = sys.argv[1]
    data_file = sys.argv[2]
    if(not os.path.isfile(script_file)):
        raise ValueError("Invalid script file")
    else:
        script_bytes = os.path.getsize(data_file)
        if(script_bytes is 0):
            raise ValueError("Script file is empty")
        else:
            # Read binary
            in_script=open(script_file, "rb")
            script_string=in_script.read()
            in_script.close()
    if(not os.path.isfile(data_file)):
        raise ValueError("Invalid data file")
    else:
        data_bytes = os.path.getsize(data_file)
        if(data_bytes is 0):
            raise ValueError("Data file is empty")



    call(["aws", "s3", "cp", script_file, global_bucket])
    call(["aws", "s3", "cp", data_file, global_bucket])

    data_bytes_start = 0
    data_bytes_end = 0
    data_bytes_chunk = data_bytes/(lambda_instances/replication)

    #call_string = \
    #            "aws lambda invoke \
    #            --invocation-type RequestResponse \
    #            --function-name lambda-map \
    #            --region us-west-2 \
    #            --log-type Tail" \
    #            +\
    #            generate_payload(script_string,data_bytes_start,data_bytes_end,0)
    #print call_string 
    #return
    #generate_payload(script_string, data_bytes_start,data_bytes_end,1)

    for i in range(lambda_instances):
        data_bytes_end = (i+1)*data_bytes_chunk
        #g_generate_payload(script_string,data_bytes_start,data_bytes_end,i)
        call_string = \
                "aws lambda invoke \
                --invocation-type RequestResponse \
                --function-name hello-lambda \
                --region us-west-2 \
                --log-type Tail" \
                +\
                str(g_generate_payload(script_string,data_bytes_start,data_bytes_end,i))\
                + "outfile.txt"
        #print call_string
        #return 
        print call_string

        subprocess.Popen(call_string,shell=True)


        #call(["aws", "lambda", "invoke", 
        #"--invocation-type RequestResponse", 
        #"--function-name lambda-map", 
        #"--region us-west-2", 
        #"--log-type Tail ", 
        #"--payload"+generate_payload(script_string,\
        #    data_bytes_start,data_bytes_end,i)])

        print "Running index->" + str(i) 

        #run lambda here lambda(bytes_start,bytes_end)
        data_bytes_start = data_bytes_end

if __name__ == "__main__":
    main()

