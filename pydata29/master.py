#!/usr/bin/env python
import copy
import concurrent.futures
import processor
import io_handler

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def chunk_generator(g, n):
    """Yield successive n-sized chunks from generator g."""
    i = 0
    chunk = []
    for el in g:
        chunk += [el]
        i += 1
        if i % n == 0:
            yield copy.deepcopy(chunk)
            chunk = []
    if chunk:
        yield chunk

def concurrent_map(fn, data,
                   executor_class = concurrent.futures.ProcessPoolExecutor,
                   executor_params = {}):
    with executor_class(**executor_params) as executor:
        return executor.map(fn, data)

def concurrent_reduce(fn, data, chunk_size = 100):
    size = chunk_size
    reduced = [d for d in copy.deepcopy(data) if d is not None]
    while len(reduced) > 1:
        data_chunks = list(chunk_generator(reduced, size))
        print("Reducing {} chunks".format(len(data_chunks)))
        reduced = list(concurrent_map(fn, data_chunks))
        size /= 10
        size = size or 1
    return reduced[0]

def process_locally():
    filepaths = io_handler.list_file_paths()
    concurrent_map(processor.process_part, filepaths)

def reduce_locally():
    totals_filepaths = map(lambda f: io_handler.file_path_to_output_path(f, 'totals'), filepaths)
    totals_series = map(io_handler.load_totals_csv_as_series, totals_filepaths)
    return concurrent_reduce(processor.reduce_series_list, totals_series)

if __name__ == '__main__':
    process_locally()
