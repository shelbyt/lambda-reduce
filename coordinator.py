import requests
import sys
import os
import os.path
from subprocess import call

lambda_instances = 4
replication = 1
global_bucket = "s3://shelby-lambda-in"

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
            in_script=(script_file, "rb")
            script_string=script_open.read()
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
    data_bytes_chunk = data_bytes/(lambda_instance/replication)

    for i in range(lambda_instances):
        data_bytes_end = (i+1)*data_bytes_chunk

        call(["aws", "lambda", "invoke", data_file, global_bucket])
        
        #run lambda here lambda(bytes_start,bytes_end)
        data_bytes_start = data_bytes_end

if __name__ == "__main__":
    main()

