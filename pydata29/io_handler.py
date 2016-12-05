#!/usr/bin/env python
import re
import boto3
import pandas

defaults = {
    'data_root': 'tmp/data',
    'prefix': 'signalmedia100s-',
    'filepath_format': '{data_root}/{prefix}{num:0{width}}',
    'width': 4,
    'articles_folder': 'articles',
    'totals_folder': 'totals',
}

re_s3_protocol_prefix = re.compile(r"^s3:\/\/")

s3 = None
def s3_client():
    global s3
    if s3 is None:
        s3 = boto3.client('s3')
    return s3

lamb = None
def lambda_client():
    global lamb
    if lamb is None:
        lamb = boto3.client('lambda')
    return lamb

def describe_file_path(filepath):
    path   = filepath.split('/')
    name   = path[-1].split('.')[0]
    root   = '/'.join(path[:-1])
    parent = '/'.join(path[:-2])
    description = {
        'path' : path,
        'name' : name,
        'root' : root,
        'root_parent' : parent,
    }
    if root.startswith('s3://'):
        bucket = re_s3_protocol_prefix.sub('', root).split('/')
        description['bucket'] = bucket[0]
        description['key'] = '{}/{}'.format('/'.join(bucket[1:]), path[-1])
    return description

def list_file_paths(data_root       = defaults['data_root'],
                    prefix          = defaults['prefix'],
                    filepath_format = defaults['filepath_format'],
                    width           = defaults['width']):
    return [filepath_format.format(
                data_root = data_root,
                prefix    = prefix,
                num       = num,
                width     = width
            ) for num in range(10000)]

def load_jsonl_as_pandas(file_path, options = {}):
    source = file_path
    file = describe_file_path(file_path)
    if file.get('bucket'):
        r = s3_client().get_object(Bucket=file['bucket'],
                                   Key=file['key'])
        source = r['Body'].read().decode('utf-8')
    return pandas.read_json(source, lines=True, **options)

def file_path_to_output_path(file_path, folder='totals'):
    file = describe_file_path(file_path)
    return '{}/{}/{}'.format(
        file['root_parent'],
        folder,
        file['name']
    )

def load_totals_csv_as_series(file_path):
    try:
        return pandas.read_csv(file_path, sep=';', squeeze=True, index_col='agg')
    except:
        return None

def load_articles_csv_as_df(file_path):
    try:
        return pandas.read_csv(file_path, sep=';', index_col='id').fillna(0)
    except:
        return None

def write_pandas_to_csv(pandas_object, path, options={}):
    print("Writing to path {}...".format(path))
    if path and re_s3_protocol_prefix.search(path):
        file = describe_file_path(path)
        bucket = file['bucket']
        key = file['key']
        csv = pandas_object.to_csv(None, **options)
        s3 = s3_client()
        print("Writing file {} to S3 bucket {}".format(
            key,
            bucket
        ))
        response = s3.put_object(Body   = csv,
                                 Bucket = bucket,
                                 Key    = key)
    else:
        print("Writing file {} to filesystem.".format(path))
        pandas_object.to_csv(path, **options)
    print("Wrote to path {}!".format(path))
    return path
if __name__ == '__main__':
    from pprint import PrettyPrinter
    pp = PrettyPrinter()

# if running from root in repl
    if False:
        import setup
        from loader import *

        pp.pprint('listing file paths...')
        file_paths = list_file_paths()
        pp.pprint(len(file_paths))
        pp.pprint(file_paths[-1])

        pp.pprint('loading file paths...')
        part_index = 0
        part = load_jsonl_as_pandas(file_paths[part_index])
        pp.pprint('sample loaded...')
        pp.pprint(part)

