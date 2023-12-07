'''
The masterserver cmanages the chunkservers, provides the metadata to clients when requested, and handles
failures and replication. It also writes a log to disk so it can recover all in-memory data through reading the log after a crash.
'''

import os
import pickle
import random
import threading
import time
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

def FileInfo(deleted, time, chunk_list):
    deleted = False
    deleted_time = None # float deletion time
    chunk_list = [] # list[(int,int)] chunkIds and versionNumbers

random.seed(0)
num_replicas = 5
lease_duration_secs = 30
deleted_file_duration_secs = 30
chunkserver_url_to_proxy = {}
filename={}
filename_to_chunks = {} # string filename -> FileInfo
chunk_to_filename = {} # int chunkId -> string filename
chunk_to_urls = {} # int chunkId -> list[string] urls of replicas
chunk_to_primary = {} # int chunkId -> string url of primary
chunk_id_counter = 500 # start at 1000 to differentiate from chunk indexes
root_dir = 'C:\\Users\\aadit\\OneDrive\\Desktop\\Os_p\\OS-Project-DFS\\temp\\master'
data_dir= 'server_data/'
os.makedirs(data_dir, exist_ok=True)
thread_interval = 30
url_to_heartbeat_time = {} # string url -> float time of last hearbeat
print('master initialized')

'''
init_from_log:
Initializes the master from its log, if it exists. Used to recover in-memory metadata after a master crash. 
It recovers its chunk to replica mapping by asking all chunkservers for what chunks they own.
'''

def init_from_log():
    if not os.path.isfile(root_dir + 'log.txt'):
        print('Initializing from scratch')
        return
    print('Initializing from log')
    chunkserver_urls = []

    with open(root_dir + 'log.txt', 'rb') as f:
        chunkserver_urls = pickle.load(f)
        filename_to_chunks = pickle.load(f)
        chunk_id_counter = pickle.load(f)

    for filename in filename_to_chunks:
        chunk_list = filename_to_chunks[filename].chunk_list
        for chunk_id, version in chunk_list:
            chunk_to_filename[chunk_id] = filename

    for url in chunkserver_urls:
        cs_proxy = ServerProxy(url)
        try:
            chunks_on_chunkserver = cs_proxy.get_chunks()
            # if master crashed and rebooted, it loses its lease mapping in memory
            cs_proxy.remove_current_leases()
        except:
            # chunkserver is down
            continue
        chunkserver_url_to_proxy[url] = cs_proxy
        for chunk_id, version in chunks_on_chunkserver:
            if chunk_id not in chunk_to_urls:
                chunk_to_urls[chunk_id] = []
                chunk_to_urls[chunk_id].append(url)

'''
flush_to_log:
Helper function to flush necessary in-memory data to disk.
'''

def flush_to_log():
    with open(root_dir + 'log.txt', 'wb') as f:
        pickle.dump(list(chunkserver_url_to_proxy.keys()), f)
        pickle.dump(filename_to_chunks, f)
        pickle.dump(chunk_id_counter, f)
    print('flushed metadata to log')

'''
check_heartbeats:
Runs periodically in the background thread to check what chunkservers have not sent
heartbeats recently. If a chunkserver has not sent a heartbeat, we assume it is down
and remove it from our metadata.
    '''
def check_heartbeats():
    urls_to_delete = []
    for url in url_to_heartbeat_time:
        last_heartbeat_time = url_to_heartbeat_time[url]
        # set expiration of heartbeat to be thread interval + delta
        heartbeat_expiration = last_heartbeat_time + thread_interval + 5
        if time.time() > heartbeat_expiration:
            print('did not receive heartbeat from', url)
            remove_chunkserver(url)
            urls_to_delete.append(url)
    
    for url in urls_to_delete:
         del url_to_heartbeat_time[url]

'''
heartbeat:
Collects heartbeats from a chunkserver and notes the time the heartbeat was received.
The server also sends a list of chunks it owns, and the master will respond with
the chunks that it no longer has metadata for. The chunkserver will then delete these
chunks.

url(string): url of chunkserver that's sending this heartbeat
chunk_ids(list[int]): list of chunk ids that this chunkserver owns
list[int]: list of chunk ids that the master no longer has metadata for
'''
def heartbeat(url, chunk_ids):
    print('received heartbeat from', url)
    url_to_heartbeat_time[url] = time.time()

        # if we're hearing again from a server we thought was down:
    if url not in chunkserver_url_to_proxy:
        chunkserver_url_to_proxy[url] = ServerProxy(url)
            
            # add this replica url back into chunk to url mapping
        deleted_chunk_ids = link_with_master(url, chunk_ids)
        if len(deleted_chunk_ids) > 0:
                print('does not have metadata for:', deleted_chunk_ids)
        return deleted_chunk_ids

        # otherwise, just scan for deleted chunk ids
    deleted_chunk_ids = []
    for chunk_id, version in chunk_ids:
        if chunk_id not in chunk_to_filename:
            deleted_chunk_ids.append(chunk_id)
    if len(deleted_chunk_ids) > 0:
        print('does not have metadata for:', deleted_chunk_ids)
    return deleted_chunk_ids

'''
garbage_collect:
Runs periodically in the background to check all filenames that have been deleted.
If the file has been deleted (renamed to DELETED_original_filename) and the deletion
wait time has passed, we delete its metadata.
'''
def garbage_collect():
    #iterate through file namespace and remove old deleted files
    filenames_to_delete = []
    for filename in filename_to_chunks:
        f = filename_to_chunks[filename]
        if f.deleted:
            if time.time() > f.deleted_time + deleted_file_duration_secs:
                filenames_to_delete.append(filename)
    if len(filenames_to_delete) > 0:
        print('deleting filenames:', filenames_to_delete)
    for filename in filenames_to_delete:
        del filename_to_chunks[filename]

    #iterate through chunk namespace and remove orphaned chunks
    chunks_to_delete = []
    for chunkId in chunk_to_filename:
        f = chunk_to_filename[chunkId]
        deleted_f = 'DELETED_' + f
        if f not in filename_to_chunks and deleted_f not in filename_to_chunks:
            chunks_to_delete.append(chunkId)
        if len(chunks_to_delete) > 0:
            print('deleting chunks:', chunks_to_delete)
    for chunkId in chunks_to_delete:
        del chunk_to_filename[chunkId]
        del chunk_to_urls[chunkId]
        # When master crashes, chunk_to_primary is lost, so this may be gone from memory
        if chunkId in chunk_to_primary:
            del chunk_to_primary[chunkId]

    if len(filenames_to_delete) > 0 or len(chunks_to_delete) > 0:
        flush_to_log()
    print('after garbage collection:', filename_to_chunks, chunk_to_filename)

'''
background_thread:
Thread that continuously runs to handle various background tasks.

'''

def background_thread(interval):
    while True:
        garbage_collect()
        check_heartbeats()
        rereplicate_chunks()
        time.sleep(interval)
background_thread = threading.Thread(target=background_thread, args=[thread_interval])
background_thread.daemon = True
background_thread.start()

'''
lookup_chunk_version:
Looks up the version of the specified chunk id. Used for stale replica detection.

@param chunk_id(int): chunk id to get version for
@return (int, int): version of the chunk and its chunk index
The chunk index is used when the master needs to update its own metadata.
'''
def lookup_chunk_version(chunk_id):
    filename = chunk_to_filename[chunk_id]
    file_info = filename_to_chunks[filename]
    cur_version = None
    chunk_idx = None
    for i, (cur_id, v) in enumerate(file_info.chunk_list):
        if cur_id == chunk_id:
            cur_version = v
            chunk_idx = i
            break
    return cur_version, chunk_idx



'''
link_with_master:
A chunkserver calls this when it starts to link with the master. The master will then
add this chunkserver and its chunks to its own metadata. Since this will be called when
a replica recovers after crashing, it also checks versions in the chunk list for any
stale chunks, and reports them if found.

url(string): url of chunkserver sending this request
chunk_list(list[int]): chunk ids that this chunkserver owns
list[int]: chunk ids that are stale or deleted
'''
def link_with_master(url, chunk_list):
    url_to_heartbeat_time[url] = time.time()
    chunkserver_url_to_proxy[url] = ServerProxy(url)
    stale_chunks = []
    for chunk_id, version in chunk_list:
        if chunk_id not in chunk_to_filename:
            stale_chunks.append(chunk_id)
            continue
        cur_version, chunk_idx = lookup_chunk_version(chunk_id)
        if cur_version == None:
            return 'chunk id not found in master metadata'
        # check if version number is stale. if it is, return list of stale chunks
        if version < cur_version:
            # stale replica
            stale_chunks.append(chunk_id)
            # don't add this url to list of replicas for chunk
            continue
        if version > cur_version:
            # master is out of date, so we take the newer version
            filename_to_chunks[filename].chunk_list[chunk_idx] = (chunk_id, version)
        if chunk_id not in chunk_to_urls:
            chunk_to_urls[chunk_id] = []
        if url not in chunk_to_urls[chunk_id]:
            chunk_to_urls[chunk_id].append(url)
    flush_to_log()
    print('chunkserver_url_to_proxy:', chunkserver_url_to_proxy)
    return stale_chunks

'''
remove_chunkserver:
Removes the specified chunkserver from the master's metadata. Usually called when we
detect that this chunkserver has failed or has stopped responding.

url_to_remove(string): url of failed chunkserver to remove from metadata
'''
def remove_chunkserver(url_to_remove):
    # remove url of failed chunkserver from chunk_to_urls and chunkserver_url_to_proxy
    if url_to_remove not in chunkserver_url_to_proxy:
        return
    del chunkserver_url_to_proxy[url_to_remove]
    for chunk_id in chunk_to_urls:
        replica_list = chunk_to_urls[chunk_id]
        new_list = []
        for replica_url in replica_list:
            if replica_url == url_to_remove:
                continue
            new_list.append(replica_url)
        chunk_to_urls[chunk_id] = new_list
    print('removed failed chunkserver at', url_to_remove)
    print('remaining chunk_to_urls:', chunk_to_urls)

'''
report_invalid_checksum:
Called by a chunkserver to report a chunk that it discovers has an invalid checksum.
The master then rereplicates that chunk to a different chunkserver.

chunk_id(int): chunk id of chunk with invalid checksum
bad_url(string): url of chunkserver with bad chunk
'''
def report_invalid_checksum(chunk_id, bad_url):
    print(bad_url, 'has invalid checksum for', chunk_id, 'so rereplicating')
    all_urls = chunkserver_url_to_proxy.keys()
    valid_replicas = chunk_to_urls[chunk_id]
    urls_without_replicas = [url for url in all_urls if url not in valid_replicas]
    valid_replicas.remove(bad_url)
    chunk_to_urls[chunk_id] = valid_replicas
    replicate_single_chunk(chunk_id, valid_replicas, urls_without_replicas)

'''
replicate_single_chunk:
Replicates a single chunk from an existing replica to a replica without that chunk.

chunk_id(int): chunk id to replicate
replica_list(list[string]): list of urls that have a valid replica of this chunk
urls_without_replicas(list[string]): list of urls without a replica of this chunk
'''
def replicate_single_chunk(chunk_id, replica_list, urls_without_replicas):
    while len(urls_without_replicas) > 0:
        url = urls_without_replicas.pop()
        # pick random replica to copy from
        replica = random.choice(replica_list)
        cs_proxy = chunkserver_url_to_proxy[url]
        version, _ = lookup_chunk_version(chunk_id)
        res = cs_proxy.replicate_data(chunk_id, version, replica)
        if res == 'success':
            chunk_to_urls[chunk_id].append(url)
            print('re-replicated chunk', chunk_id, 'from', replica, 'to', url)
            break
        elif len(urls_without_replicas) == 0:
            print('not enough chunkservers to replicate chunk')
            break
        else:
            print('chunk replication failed:', res, 'trying on different replica')

'''
rereplicate_chunks:
Periodically called to rereplicate all chunks with # replicas fewer
than self.num_replicas.
'''
def rereplicate_chunks(self):
    # loop through self.chunk_to_urls
    # for every chunk id with < self.num_replicas replicas
    #   replicate this chunk to another chunkserver
    for chunk_id in self.chunk_to_urls:
        replica_list = self.chunk_to_urls[chunk_id]
        if len(replica_list) >= self.num_replicas:
            continue
        all_urls = self.chunkserver_url_to_proxy.keys()
        # get all urls that don't contain a replica for this chunk
        urls_without_replicas = [url for url in all_urls if url not in replica_list]
        for i in range(len(replica_list), self.num_replicas):
            self.replicate_single_chunk(chunk_id, replica_list, urls_without_replicas)

'''
create:
Called by the client to create a file with the desired filename.
Sets up metadata for the file and creates it on a replicated number of chunkservers.

@param filename(string): desired filename of file to create
@return string: result message
'''
def create(filename):
    #randomly sample self.num_replicas servers to host replicas on
    all_urls = chunkserver_url_to_proxy.keys()
    proxy_urls = random.sample(all_urls, num_replicas)
    #assign next chunkId to each chunk sequentially
    chunk_id = chunk_id_counter
    replica_urls = []
    for url in proxy_urls:
        proxy = chunkserver_url_to_proxy[url]
        try:
            proxy.create(filename, chunk_id)
        except Exception as e:
            print('create failed:', e)
            remove_chunkserver(url)
            continue
        replica_urls.append(url)
    #store chunkId->list[server] mapping in chunk_to_url
    chunk_to_urls[chunk_id] = replica_urls
    chunk_to_filename[chunk_id] = filename
    chunk_id_counter += 1
    #append this chunkId to list of chunkIds in filename_to_chunks
    if filename not in filename_to_chunks:
        filename_to_chunks[filename] = FileInfo(chunk_list=[])
    filename_to_chunks[filename].chunk_list.append((chunk_id, 0))
    print('after CREATE filename_to_chunks:', filename_to_chunks)
    print('chunk_to_urls:', chunk_to_urls)
    flush_to_log()
    return 'successfully created ' + filename

'''
delete:
Called by the client to delete the specified filename. All the master does is
rename the filename to DELETED_original_filename. The background process will
delete the metadata for the file after the specified deletion time expires.

filename(string): desired filename to delete
string: result message
'''
def delete(filename):
    if filename not in filename_to_chunks:
        return 'file not found'
    chunk_list = filename_to_chunks[filename].chunk_list[:]
    del filename_to_chunks[filename]
    t = time.time()
    deleted_filename = 'DELETED_' + filename
    filename_to_chunks[deleted_filename] = FileInfo(True, t, chunk_list)
    print('after DELETE filename_to_chunks:', filename_to_chunks)
    flush_to_log()
    return 'successfully deleted ' + filename

'''
read:
Called by the client to get metadata for a chunkserver read.

filename(string): filename of desired file to read
chunk_idx(int): desired chunk index to read within file
(int, list[string]): the chunk id of the (filename, chunk_idx) pair
and the list of all replicas that this chunk id is located on.
'''
def read(filename, chunk_idx):
    if filename not in filename_to_chunks:
        return 'file not found'
    if chunk_idx > len(filename_to_chunks[filename].chunk_list) - 1:
            return 'requested chunk idx out of range'
    chunk_id, version = filename_to_chunks[filename].chunk_list[chunk_idx]
    replica_urls = chunk_to_urls[chunk_id]
    print('READ returning:', chunk_id, replica_urls)
    return (chunk_id, replica_urls)

'''
get_primary:
Called by the client to get lease information for a given filename and chunk index.

filename(string): filename of desired file to get primary information for
chunk_idx(int): desired chunk index to get primary information for
force_new_primary: whether or not the client wishes to force the master to
provide a new primary instead of returning the existing primary. This is done when the
client detects that a primary is down.
'''
def get_primary(filename, chunk_idx, force_new_primary):
    if filename not in filename_to_chunks:
        return 'file not found'
    chunk_id, version = filename_to_chunks[filename].chunk_list[chunk_idx]
    replica_urls = chunk_to_urls[chunk_id]

    primary_timed_out = True
    # see if we've already assigned a primary to this chunk
    if chunk_id in chunk_to_primary:
        res = chunk_to_primary[chunk_id]
        original_cache_timeout = res[1]
        if time.time() <= original_cache_timeout:
            primary_timed_out = False
                # leases are refreshed through heartbeat messages, not write requests
        # if we didn't find a primary, or client tells us to, pick a new primary
    if force_new_primary or primary_timed_out:
        print('picking new primary for chunk id', chunk_id)

        # update version number of chunk and notify replicas
        chunk_id, version = filename_to_chunks[filename].chunk_list[chunk_idx]
        filename_to_chunks[filename].chunk_list[chunk_idx] = (chunk_id, version+1)
        urls_to_remove = []
        for url in replica_urls:
            try:
                replica_proxy = chunkserver_url_to_proxy[url]
                replica_proxy.update_version(chunk_id, version+1)
            except:
                urls_to_remove.append(url)
                remove_chunkserver(url)
                continue
        for url in urls_to_remove:
            replica_urls.remove(url)

        # choose a replica as primary
        while len(replica_urls) > 0:
            primary_url = random.choice(replica_urls)
            primary_proxy = chunkserver_url_to_proxy[primary_url]
            timeout = time.time() + lease_duration_secs
            try:
                primary_proxy.assign_primary(chunk_id, timeout)
                break
            except:
                remove_chunkserver(primary_url)
                replica_urls = chunk_to_urls[chunk_id]
            if len(replica_urls) == 0:
                return 'no replicas remaining for chunk id ' + str(chunk_id)
            chunk_to_primary[chunk_id] = [primary_url, timeout]
            flush_to_log()
        else:
            print('reused old primary for chunk id', chunk_id)
        return (chunk_id, chunk_to_primary[chunk_id][0], replica_urls)

def main():
    master_server = SimpleXMLRPCServer(('localhost', 9000), allow_none=True)
    master_server.register_introspection_functions()
    master_server.register_instance(Master())
    master_server.serve_forever()

if __name__ == '__main__':
    main()