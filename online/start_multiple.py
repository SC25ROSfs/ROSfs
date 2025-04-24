#!/usr/bin/python3
import sys
import subprocess
import concurrent.futures


def run_subprocess():
    process = subprocess.Popen(['python3', 'listener.py', '127.0.0.1', 'image1c', '1'], stdout=subprocess.PIPE)        

    output, error = process.communicate()
    return output.decode('utf-8')

if __name__=='__main__':
    if len(sys.argv) < 2:
        sys.exit("Usage: python3 start_multiple.py thread_num")
    thread_num = int(sys.argv[1])
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_results = [executor.submit(run_subprocess) for _ in range(thread_num)]
        
        for future in concurrent.futures.as_completed(future_results):
            output = future.result()
            print(output)