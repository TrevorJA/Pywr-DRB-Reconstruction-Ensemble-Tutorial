#!/bin/bash
#SBATCH --job-name=reconstruction
#SBATCH --output=./logs/reconstruction.out
#SBATCH --error=./logs/reconstruction.err
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=40


### SETUP
module load python/3.11.5
source venv/bin/activate

# number of processes available for MPI
np=$(($SLURM_NTASKS_PER_NODE * $SLURM_NNODES))

# Number of realizations to run
# assume we run the first N realizations
N_REALIZATIONS=${N_REALIZATIONS:-100}

### Calculate catchment inflow
echo "Calculating catchment inflow for the first $N_REALIZATIONS realizations"
python 02_calculate_catchment_inflow.py $N_REALIZATIONS

### Predict inflows (this uses MPI)
echo "Predicting inflows for the first $N_REALIZATIONS realizations"
mpirun -np $np python 03_predict_inflows.py $N_REALIZATIONS

### Run the reconstruction ensemble (this uses MPI)
echo "Running the reconstruction ensemble for the first $N_REALIZATIONS realizations"
mpirun -np $np python 04_run_pywrdrb_simulations.py $N_REALIZATIONS