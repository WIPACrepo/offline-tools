
import argparse

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help = "GRL file of CR/IceTop WG", type = str)
    args = parser.parse_args()

    good_runs = []

    with open(args.file, 'r') as f:
        for line in f:
            line = line.strip()

            if len(line) == 0:
                continue

            columns = line.split()

            if len(columns) < 4:
                continue

            if not is_int(columns[3]) or not is_int(columns[0]):
                continue

            if int(columns[3]):
                good_runs.append(str(columns[0]))

    print "Found %s good runs" % len(good_runs)
    print ' '.join(good_runs)

            
