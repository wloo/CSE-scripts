import numpy as np
import matplotlib.pyplot as plt
from mpi4py import MPI

from P4_serial import *

def slave(comm):
    while 1:
        # Receive the tuple from the master
        pair = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag(): break  # If the tag is the dietag(1), then break
        # Otherwise keep calculating
        slave_row = pair[0] # The first value is the row number
        slave_y = pair[1] # the second is the y-value
        # Calculate across the row
        i = []
        for j,x in enumerate(np.linspace(minX, maxX, width)):
            i.append(mandelbrot(x,slave_y))
        # Send the row back to the master
        comm.send(i, dest=0, tag=slave_row)
    
def master(comm):
    # Make an iterable for the rows
    y_values = enumerate(np.linspace(minY, maxY, height))
        
    # Initialize the final matrix
    image = np.zeros([height,width], dtype=np.uint16)
    
    # Send initial work to each process
    for p in xrange(1,size):
        current = y_values.next()
        comm.send(current, dest=p, tag=worktag)
    
    # Iterate over all rows
    received_rows = 0 # keep track of how many rows have come back
    row = current[0]
    while received_rows < height: # When we're still waiting to receive
        i_data = comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG, status=status)
        i_row = status.Get_tag()
        i_rank = status.Get_source()
        image[i_row,:] = i_data # Write the row from that process to the final matrix
        received_rows += 1
        
        if row < (height-1): # Send that process more work
            current = y_values.next()
            comm.send(current, dest=i_rank, tag=worktag)
            row = current[0]
    
    # Shutdown all the processes
    for p in xrange(1, size):
       comm.send(None, dest=p, tag=dietag)

    return image


if __name__ == '__main__':
    # Get MPI data
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    
    # Set up worktag and dietag
    worktag = 0
    dietag = 1
    
    if rank == 0:
        start_time = MPI.Wtime()
        C = master(comm)
        end_time = MPI.Wtime()
        print "Time: %f secs" % (end_time - start_time)
        plt.imsave('Mandelbrot.png', C, cmap='spectral')
        #plt.imshow(C, aspect='equal', cmap='spectral')
       # plt.show()
    else:
        slave(comm)