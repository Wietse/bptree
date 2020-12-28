import sys
import uuid


def gen_kv_pair():
    while True:
        k = uuid.uuid4()
        v = uuid.uuid4()
        yield k.int, v.int


def generate_data(n=1000, fname='dataset.csv'):
    with open(fname, 'w') as fh:
        print('key,value', file=fh)
        for i, (k, v) in enumerate(gen_kv_pair()):
            if i >= n:
                break
            print(f'{k},{v}', file=fh)


if __name__ == '__main__':
    n = 1000
    fname = f'dataset{n}.csv'
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
        if len(sys.argv) > 2:
            fname = sys.argv[2]
        else:
            fname = f'dataset{n}.csv'
    generate_data(n, fname)
