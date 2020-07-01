# Using Standard Libraries, Python 3.6.9
from multiprocessing import Pool
from time import time, strftime
# Note that subprocess is much improved in Python 3.7
from subprocess import run, PIPE

def compute(pxb):
    # computes b^x (mod p)
    # Pool method map() requies a function with only one input, so 
    #   inputs changed to a tuple.
    # This also forced a couple of changes to how the values and returns were handled below.
    p = pxb[0]
    x = pxb[1]
    b = pxb[2]
    if (x == 0):
        return (p, x, 1)
    elif (x == 1):
        return (p, x, (b % p))
    elif (x % 2 == 1):
        value = compute((p, (x-1)/2, b**2))[2] % p
        return (p, x, (b*value) % p)
    else:
        value = compute((p, x/2, b**2))[2] % p
        return (p, x, value)

# This if is needed to run multiprocessing
if __name__ == '__main__':
    # Setup to gracefully restart if a prior run was stopped
    # First get the list of all result files
    dirListing = run(['ls', 'mp_out',], stdout=PIPE, universal_newlines=True)
    files = dirListing.stdout.splitlines()
    # Sort so that we're sure the last one in the list is the latest computation
    files.sort()
    lastFile = files[-1]
    # Now open that file, and go the end of that, last prime.
    findEnd = open('mp_out/' + lastFile, 'r')
    endLines = findEnd.readlines()
    endLine = endLines[-1]
    endLine = endLine.strip()
    lastPrime = endLine.split(" ")[0]
    # So this is the last prime we stored the results for.
    lastComputation = int(lastPrime)

    # Keep the file open so we can read lines as needed.
    f = open("indices.txt","r")
    # Now, read lines until we get past the point where we stopped computing.
    primeRead = 0
    while primeRead < lastComputation:
        line = f.readline()
        newLine = line.strip()
        entries = newLine.split(" ")
        primeRead = int(entries[0])
    print("Restarting computation after prime", primeRead)
    # Now, the pointer in the file f (indices.txt) is where we left off.
    while True:
        # Reset the list of lines to empty
        lines = []
        # Read 1000 lines, pointer stays where we left off by default
        for n in range(1000):
            lines.append(f.readline())
        # Convert lines to irregular pairs
        pairs = []
        for lineNum, line in enumerate(lines):
            newLine = line.strip()
            entries = newLine.split(" ")
            if len(entries) != 1:
                for i in range(len(entries) - 1):
                    pairs.append((int(entries[0]), int(entries[i+1]) - 1, 2))        
        # Keep track of computation time
        t1 = time()
        # This is where the actual comptuation/multiprocessing takes place
        # maxtasksperchild means how many times we run the compute() function before
        #   cleaning up memory.  Cleaning up every time might be overly aggressive.
        with Pool(processes=4, maxtasksperchild = 1) as pool:
            results = pool.map(compute, pairs)
            pool.close()
            pool.join()
        # Keeping track of only the computation time, not the file handling time.
        # Notice that this is real time, processing time is ~4x this much, because
        #   we are running 4 parallel processes.
        totalTime = round(time() - t1, 5)
        # This is a double-check, we match inputs to outputs to make sure that we
        #   we didn't miss any inputs, and compute any missed inputs.
        failedReps = 0
        while len(pairs) > 0:
            for result in results:
                if (result[0], result[1], 2) in pairs:
                    pairs.remove((result[0], result[1], 2))
                if result[2] == 1:
                    print("FOUND!!!", result[0], result[1], flush=True)
                    found = open('FOUND.txt', 'a')
                    print(result[0], result[1], file=found, flush=True)
                    found.close()
            if len(pairs) > 0:
                print("ERROR, one or more pairs not computed, repairing...")
                print(pairs)
                for pair in pairs:
                    results.append(compute(pair))
            failedReps += 1
            if failedReps > 5:
                print("Unable to repair after five attempts. Aborting.")
                quit()
        # Dump complete results to text files to make verification easier.
        filename = 'mp_out/output_mp_' + str(results[0][0]).zfill(10) + '.txt'
        fout = open(filename, 'w')
        print("Computation time:", totalTime, "seconds", file=fout, flush=True)
        for result in results:
            print(result[0], result[1], result[2], file=fout, flush=True)
        fout.close()
        # Update user every 1000 lines.
        print("Completed section starting at prime", results[0][0], "in", totalTime, "seconds at", strftime("%d %b %H:%M:%S"))
    # Put this here to feel better, but we'll never get here.
    f.close()

