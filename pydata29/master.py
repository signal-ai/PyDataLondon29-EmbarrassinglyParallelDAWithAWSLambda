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

def results(filepaths):
    totals_filepaths = [io_handler.file_path_to_output_path(f, 'totals') for f in filepaths]
    articles_filepaths = [io_handler.file_path_to_output_path(f, 'articles') for f in filepaths]
    yield processor.reduce_series_list(
        map(io_handler.load_totals_csv_as_series, totals_filepaths)
    )
    yield processor.concat_dataframe_list(
        map(io_handler.load_articles_csv_as_df, articles_filepaths)
    )

def dump_results(generator, root):
    io_handler.write_pandas_to_csv(next(generator),
                                   '{}/totals.csv'.format(root),
                                   {'index_label': 'agg',
                                    'header': ['count'],
                                    'index': True,
                                    'sep': ';'})
    io_handler.write_pandas_to_csv(next(generator),
                                   '{}/processed_articles.csv'.format(root),
                                   {'index_label' : 'id',
                                    'index' : True,
                                    'sep' : ';'})

def process_locally():
    filepaths = io_handler.list_file_paths()[:10]
    return concurrent_map(processor.process_part, filepaths)

def process_lambda(concurrency = 801):
    filepaths = io_handler.list_file_paths(data_root='s3://pydata-29/data')[:10]
    return concurrent_map(processor.process_filepath_in_lambda, filepaths,
                          executor_class=concurrent.futures.ThreadPoolExecutor,
                          executor_params={'max_workers': concurrency})

if __name__ == '__main__':
    import sys
    architecture = sys.argv[1].strip() if len(sys.argv) >= 2 else 'local'
    concurrency = int(sys.argv[2]) if len(sys.argv) >= 3 else 801
    if  'lambda' in architecture:
        res = process_lambda(concurrency)
    else:
        res = process_locally()
    [r for r in res]
    print("Done")


    # results_root = '{}/results'.format(
        # io_handler.describe_file_path(filepaths[0])['root_parent']
    # )
    # dump_results(
        # results(filepaths),
        # results_root
    # )
