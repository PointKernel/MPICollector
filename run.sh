#!/bin/bash
#SBATCH -N 10
#SBATCH -C haswell
#SBATCH -q regular
#SBATCH -J mpi_io
#SBATCH -t 00:10:00

#OpenMP settings:
export OMP_NUM_THREADS=1
export OMP_PLACES=threads
export OMP_PROC_BIND=spread


#run the application:
srun -n 10 -c 64 --cpu_bind=cores /global/cscratch1/sd/yswang/toto/socket_test/socket_s 5 45
