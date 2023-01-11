import time 
import logging
import ssl 
import os 
from npmThreader import npmThreader

# Certificates stuff                            
openssl_dir, openssl_cafile = os.path.split(      
    ssl.get_default_verify_paths().openssl_cafile)
# no content in this folder
os.listdir(openssl_dir)
# non existent file
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.check_hostname = True
ssl_context.load_default_certs()

def log_init():
    from datetime import datetime 
    import pytz 

    now = datetime.now()
    now = now.replace(tzinfo=pytz.UTC)
    name = now.strftime('./logs/%Y-%m-%d--%H-%M-%S--%Z.log')
    logging.basicConfig(
        filename=name,
        filemode='a',
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    logger = logging.getLogger()

    logger.info('***** Starting *****')
    now = datetime.now()
    now = now.replace(tzinfo=pytz.UTC)
    now = now.strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f'*** Time: {now} ***') 
    return logger 

def read_all_the_npm_pkgs():
    import urllib.request 
    https_handler = urllib.request.HTTPSHandler(context=ssl_context)
    url = 'http://raw.githubusercontent.com/nice-registry/all-the-package-names/master/names.json'
    opener = urllib.request.build_opener(https_handler)
    print('Opening URL')
    start_time = time.time_ns()
    ret = opener.open(url, timeout=2)
    end_time = time.time_ns()
    print(f'Time: {(end_time - start_time) / 1000000000}')
    #logger.info(f'Time - Open URL: {(end_time - start_time) / 1000000000}')
    return ret 

def clean():
    # Clean up package names
    ret = read_all_the_npm_pkgs()
    pkgs = []
    start_time = time.time_ns()
    for object in ret:
        #if len(pkgs) >= 5000:
         #   break 
        str1 = object.decode('utf-8')
        str2 = str1.replace('\n', '')
        str2 = str2.replace(',', '')
        str2 = str2.replace('\"', '')
        str_final = str2.replace('\'', '')
        str_final.strip()
        if str_final != '[' and str_final != ']' and str_final.strip() != '--hiljson':
            pkgs.append(str_final.strip())
    end_time = time.time_ns()
    #logger.info(f'Time - Cleaning: {(end_time - start_time) / 1000000000} seconds')
    print(f'Time - Cleaning: {(end_time - start_time) / 1000000000} seconds')
    return pkgs

def get_clean_pkgs():
    pkgs = clean()
    return pkgs

def chunker(big_data, number_of_threads):
    chunks = []
    partition = int(len(big_data) / number_of_threads)
    print(f'Partition size: {partition}')
    i = 0
    j = i + partition 
    while j <= len(big_data):
        chunks.append([item for item in big_data[i:j]])
        i+=partition
        j+=partition
    
    # Deal with remainder
    if j - len(big_data) > 0:
        chunks.append([item for item in big_data[i:len(big_data)]])

    return chunks

if __name__ == '__main__':
    logger = log_init()
    pkgs = get_clean_pkgs()

    print(f'Number of packages: {len(pkgs)}')
    num_of_threads = 100
    chunks = chunker(pkgs, num_of_threads)
    threads = []
    print('Populating Threads')
    start_time = time.time_ns()
    print(len(chunks))
    for i,chunk in enumerate(chunks):
        print(i,len(chunk))
    try:
        for i in range(len(chunks)):
            new_thread = npmThreader(chunks[i], i, logger)
            threads.append(new_thread),
    except Exception as e:
        print(f'Error: {e}')
    end_time = time.time_ns()
    print(f'Total Execution time for {len(threads)} instantiations = {(end_time - start_time) / 1000000000} seconds')

    start_time = time.time_ns()
    print('Starting threads')
    for th in threads:
        th.start()

    for th in threads:
        th.join()

    print('Done with threads')
