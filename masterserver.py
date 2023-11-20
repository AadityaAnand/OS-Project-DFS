"""
masterserver.py

Master server manages all the metadata for chunkservers, provides the metadata to clients as per their request, 
and handles chunkserver failure and replication. It also writes a log persistently to disk so it is able to recover
all in-memory data by reading the log after a crash.

"""
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import os.path
import pickle
import random
import threading
import time

"""
File_Info:
Class to bundle information regarding a file. 
"""
class File_Info():
    def __init__(self, deleted=False, time=None, chunk_list=None):
        self.deleted = deleted
        self.deleted_time = time
        self.chunk_list = chunk_list if chunk_list is not None else []

"""
Master:
Class to bundle information regrading the master server.
"""
class Master():
    def __init__(self):
        random.seed(0)
        self.replicas_num = 5 #Number of replicas
        self.lease_duration = 30 #Duration of the lease
        self.deleted_file_duration = 30 #Deletion of File
        self.chunkserverurl_to_proxy = {}
        self.filename_to_chunks = {} # string filename -> File_Info
        self.chunk_filename = {} #int chunkID -> string filename
        self.chunk_urls = {} #int chunkID -> list[string] urls of replicas
        self.chunk_primary = {} #int chunkID -> string url of primary
        self.chunk_counter_id = 500 #starts at 500 to differentiate from chunk indexes
        self.root_dir = 'C:\Users\aadit\OneDrive\Desktop\Os_p\OS-Project-DFS\temp\master_data'
        self.init_log()
        self.interval_thread = 30
        self.url_heartbeat_time = {}
        background_thread = threading.Thread(target=self.bacground_thread, args=[self.interval_thread]) 
        background_thread.daemon = True
        background_thread.start()
        print('master initialized')

    """
    init_log:
    Log will be used to recover in-memory metadata after a master crash. It recovers its chuk to replica mapping by asking all chunkservers for what chunks they own. 
    """
    def init_log(self):
        #Checks if a log file exists in the directory or not
        if not os.path.isfile(self.root_dir + 'log.txt'):
            print('Initialization from scratch')
            return
        print('Initializing from log')
        chunkserver_urls = []

        #read existing data from log
        with open(self.root_dir + 'log.txt', 'rb') as f:
            chunkserver_urls = pickle.load(f)
            self.filename_to_chunks = pickle.load(f)
            self.chunk_id_counter = pickle.load(f)
        
        # intitalize self.chunk_id_to_filename to check mapping
        for filename in self.filename_to_chunks:
            chunk_list = self.filename_to_chunks[filename].chunk_list
            for chunk_id, version in chunk_list:
                self.chunk_to_filename[chunk_id]=filename
        
        #get chunks from chunkservers to create chunk while replicas mapping
        for url in chunkserver_urls:
            # Create a ServerProxy object for communication with the chunkserver
            cs_proxy = ServerProxy(url)
            # Try to get information from the chunkserver
            try:
                # Get chunks and remove current leases from the chunkserver
                chunks_at_chunkserver = cs_proxy.get_chunks()
                cs_proxy.remove_current_leases()
            # Handle the case when the chunkserver is down
            except Exception as e:
                # Log or handle the exception as needed
                print(f"Error communicating with chunkserver at {url}: {e}")
                continue
            # Update the mapping of chunkserver URL to its proxy
            self.chunkserverurl_to_proxy[url] = cs_proxy
            # Update the mapping of chunk IDs to the URLs of replicas
            for chunk_id, version in chunks_at_chunkserver:
                if chunk_id not in self.chunk_urls:
                    self.chunk_urls[chunk_id] = []
                self.chunk_urls[chunk_id].append(url)

        """
        flush_log:
        Function to push necessary in-memoroy data to disk. 
        """
        def flush_log(self):
            log_file_path = os.path.join(self.root_dir, 'log.txt')
            with open(log_file_path, 'wb') as log_file:
                # Save chunk server URLs
                pickle.dump(list(self.chunkserver_url_to_proxy.keys()), log_file)
                # Save filename-to-chunks dictionary
                pickle.dump(self.filename_to_chunks, log_file)
                # Save chunk ID counter
                pickle.dump(self.chunk_id_counter, log_file)
            print('Metadata successfully saved to log file:', log_file_path)