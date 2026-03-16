import os
import struct
import heapq
import shutil
from typing import List, Dict, Optional, Tuple

#################### Classes and Function to ease the processs ####################





class Record:
    def __init__(self, data: bytes, key: int, source_index: int = -1, ascending: bool = True):
        self.data = data
        self.key = key
        self.source_index = source_index
        self.ascending = ascending

# in sorting we will sort based on compariosn so we redefine compar,son function 
    def __lt__(self, other):
        # If ascending lower key and if descending high key
        if self.ascending:
            return self.key < other.key
        else:
            return  self.key > other.key 





class RunIterator:
    def __init__(self, filename: str, page_size: int, record_size: int):
        self.file = open(filename, 'rb')
        self.page_size = page_size
        self.record_size = record_size
        self.leftover = b''
        self.current_chunk_iter = None
        self.active = True

    def __next__(self):
        if not self.active:
            raise StopIteration

        while True:
            # If we have a current page in memory and it has records, return next
            if self.current_chunk_iter:
                try:
                    return next(self.current_chunk_iter)
                except StopIteration:
                    self.current_chunk_iter = None

            #  read the next page
            page_data = self.file.read(self.page_size)

            if not page_data:
                # No more data; leftover should only be padding
                self.leftover = b''
                self.close()
                raise StopIteration

            # combine leftover bytes from previous page with new data
            data = self.leftover + page_data

            records = []

            num_records = len(data) // self.record_size
            valid_len = num_records * self.record_size
# xxtract valid records from page (ignoring padding)
        # We read the possible max amount of data
            full_records = data[:valid_len]
            self.leftover = data[valid_len:]

            for i in range(0, len(full_records), self.record_size):
                chunk = full_records[i:i + self.record_size]
                key = struct.unpack('<I', chunk[:4])[0]
                 # rule says "product_id will never be 0"
                # Since records are continuous, we might find padding only at the very end of the file.
                if key != 0:
                    records.append(chunk)

           
            if records:
                self.current_chunk_iter = iter(records)
                return next(self.current_chunk_iter)

             # We should try reading the next page.
            continue

    def close(self):
        self.active = False
        self.file.close()







## writing records to the file
def write_records_to_file(file_obj, records: List[bytes],
                          page_size: int, record_size: int,
                          pad_final: bool = False):

    # Write the record
    for rec in records:
        file_obj.write(rec)

    # Pad the last page to complete it
    if pad_final and records:
        bytes_written = len(records) * record_size
        remainder = bytes_written % page_size
        
        if remainder != 0:
            file_obj.write(b'\x00' * (page_size - remainder))




############ PART1 CREATE RUNS ############

def generate_runs(input_filename: str, 
                  buffer_pages: int, 
                  page_size: int, 
                  record_size: int,   
                  output_dir: str, 
                  ascending: bool = True,           
                  unique: bool = False) -> List[str]:
    
    if not os.path.exists(output_dir):        
        os.makedirs(output_dir) 

    run_filenames = [] 
    run_index = 0 

    # calculate buffer capacity in terms of records r
    records_per_chunk = (buffer_pages * page_size) // record_size
    # Set read size to exactly that many bytes
    size_of_buffer_inBytes = records_per_chunk * record_size

    with open(input_filename, 'rb') as file:    
        while True: 
            # read the data which is size of available memory buffer from unsorted file 
            run_data_before_sorting = file.read(size_of_buffer_inBytes)      
            if not run_data_before_sorting:         
                break

            # extract each record from chunk of unsorted data
            records_in_run = []         
            # print(f"hangi run {run_index}")
#             print("fua:", len(records_in_run))
            for i in range(0, len(run_data_before_sorting), record_size):
                rec_bytes = run_data_before_sorting[i : i + record_size]              
                if len(rec_bytes) == record_size:
                    key = struct.unpack('<I', rec_bytes[:4])[0]
                     # Skip records with productId =0 
                     # if we try to search this keys existence in array complexity would increase
                    if key != 0:
                        records_in_run.append(Record(rec_bytes, key, ascending=ascending))
            

            if not records_in_run:
                continue  
            
            

            # sort the final list of records regarding to key 
            # discard the duplicates before or after lets do after for now 
            records_in_run.sort()

            # remove duplicates
            if unique:
                no_duplicate_final_runs = []
                if records_in_run:     
                    prev = records_in_run[0]          
                    no_duplicate_final_runs.append(prev)   
                    for current in records_in_run[1:]:
                        if current.key != prev.key:   
                            no_duplicate_final_runs.append(current)
                            prev = current
                records_in_run = no_duplicate_final_runs
            
            # Don't create empty files

            if not records_in_run:
                continue

            # write to output page            
            run_filename = os.path.join(output_dir, f"run_{run_index}.bin")
            with open(run_filename, 'wb') as run_file:
                record_bytes_list = [rec.data for rec in records_in_run]
                write_records_to_file(run_file, record_bytes_list,
                      page_size, record_size, pad_final=True)


            run_filenames.append(run_filename)
            run_index += 1

    return run_filenames






############ PART2 MERGE RUNS ############

def merge_runs(run_filenames: List[str], 
               output_filename: str, 
               buffer_pages: int, 
               page_size: int, 
               record_size: int,        
               ascending: bool = True, 
               unique: bool = False) -> None:
    
    iterators = []   
    heap = []    
    
    # Initialize iterators/heap
    for i, filename in enumerate(run_filenames):
        file_iter = RunIterator(filename, page_size, record_size) 
        iterators.append(file_iter)
        try:
            first_bytes = next(file_iter)
            key = struct.unpack('<I', first_bytes[:4])[0]
            rec_obj = Record(first_bytes, key, source_index=i, ascending=ascending)
            heapq.heappush(heap, rec_obj)
        except StopIteration:  
            continue     

    last_written_key = None         
    
    with open(output_filename, 'wb') as out_file:       
        
        while heap:
            smallest = heapq.heappop(heap)

            should_write = True
            if unique:
                if last_written_key is not None and smallest.key == last_written_key:
                    should_write = False
                else:
                    last_written_key = smallest.key

            if should_write:
            # WRITE RECORD DIRECTLY — NO PAGE BUFFERING
                out_file.write(smallest.data)

            src = smallest.source_index
            try:
                next_bytes = next(iterators[src])
                next_key = struct.unpack('<I', next_bytes[:4])[0]
                heapq.heappush(
                    heap,
                    Record(next_bytes, next_key, source_index=src, ascending=ascending)
                )
            except StopIteration:
                pass

    # PAD ONLY ONCE AT FILE END
        current_size = out_file.tell()
        remainder = current_size % page_size
        if remainder != 0:
            out_file.write(b'\x00' * (page_size - remainder))
# writeremaining records
        
    for file_iter in iterators:
        file_iter.close()




############ PART3 K-WAY MERGE ############

def external_sort(input_filename: str, 
                  output_filename: str, 
                  buffer_pages: int,    
                  page_size: int, 
                  record_size: int,     
                  ascending: bool = True, 
                  unique: bool = False) -> dict:

    #if buffer_pages < 2:
    #    print("buffer must be >2")

    # pass 0 creating runs  
    current_runs = generate_runs(input_filename, buffer_pages, page_size,   
                                 record_size, ".", ascending, unique)   
    
    initial_num_runs = len(current_runs)
    num_passes = 1
    pass_index = 1  
    
    # multi pass multiway merging
    k_ways = buffer_pages - 1
    
    while len(current_runs) > 1:
        next_pass_runs = []
        run_group_index = 0                        
        
        for i in range(0, len(current_runs), k_ways):
            group_files = current_runs[i:i + k_ways]
            merge_out_name = f"out_pass_{pass_index}_run_{run_group_index}.bin"
            merge_runs(group_files, merge_out_name, buffer_pages, page_size,         
                       record_size, ascending, unique)
            next_pass_runs.append(merge_out_name)        
            run_group_index += 1             
            
        current_runs = next_pass_runs
        pass_index += 1        
        num_passes += 1 

    # Finalize: just copy or rename the final file
    if current_runs:  
        final_intermediate = current_runs[0]
        #merge_runs([final_intermediate], output_filename, buffer_pages,   
                   #page_size, record_size, ascending, unique)  
        # If the final file is not the output filename, copy it
        if final_intermediate != output_filename:
            shutil.copy(final_intermediate, output_filename)
    else:
        # empty
        open(output_filename, 'wb').close()     

    return {
        "num_runs": initial_num_runs,   
        "num_passes": num_passes,   
        "output_file": output_filename
    }






