import os
import sys
import glob
from mpi4py import MPI
import numpy as np
import math

import warnings
warnings.filterwarnings("ignore")

import pywrdrb
from pywrdrb.utils.hdf5 import get_hdf5_realization_numbers, combine_batched_hdf5_outputs

from utils import get_parameter_subset_to_export
from config import inflow_type, input_dir, catchment_inflow_filename, start_date, end_date
from config import N_REALIZATIONS_PER_PYWRDRB_BATCH, SAVE_RESULTS_SETS, output_dir, output_filename

# Setup pathnavigator & register inflow type
pn_config = pywrdrb.get_pn_config()
pn_config[f"flows/{inflow_type}"] = os.path.abspath(input_dir)
pywrdrb.load_pn_config(pn_config)

# MPI settings
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


if __name__ == "__main__":

    # Get the number of realizations from the sys.arg inputs
    n_realizations = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    # Clear old batched output files if they exist
    if rank == 0:
        batch_pattern = f"{output_dir}/{inflow_type}_rank*_batch*.hdf5"
        model_pattern = f"{output_dir}/{inflow_type}_rank*_batch*.json"
        
        for pattern in [batch_pattern, model_pattern]:
            old_files = glob.glob(pattern)
            for file in old_files:
                if os.path.exists(file):
                    os.remove(file)
    
    comm.Barrier()  # Wait for cleanup
    
    # Get realization IDs for this ensemble set
    if rank == 0:
        realization_ids = get_hdf5_realization_numbers(catchment_inflow_filename)
        print(f"Found {len(realization_ids)} realizations, using the first {n_realizations} realizations.")
        assert n_realizations <= len(realization_ids), \
            f"Requested {n_realizations} realizations, but only found {len(realization_ids)} realizations in {catchment_inflow_filename}"
        realization_ids = realization_ids[:n_realizations]
    else:
        realization_ids = None
    
    # Broadcast realization IDs
    realization_ids = comm.bcast(realization_ids, root=0)
    
    # Split realizations into batches across ranks
    rank_realization_ids = np.array_split(realization_ids, size)[rank]
    rank_realization_ids = list(rank_realization_ids)
    n_rank_realizations = len(rank_realization_ids)
    
    # Split rank realizations into batches
    n_batches = math.ceil(n_rank_realizations / N_REALIZATIONS_PER_PYWRDRB_BATCH)
    batched_indices = {}
    for i in range(n_batches):
        batch_start = i * N_REALIZATIONS_PER_PYWRDRB_BATCH
        batch_end = min((i + 1) * N_REALIZATIONS_PER_PYWRDRB_BATCH, n_rank_realizations)
        batched_indices[i] = rank_realization_ids[batch_start:batch_end]
    
    # Run individual batches
    batch_filenames = []
    for batch, indices in batched_indices.items():
        print(f"Rank {rank}: Running simulation batch {batch+1} with {len(indices)} realizations: {indices}")
        
        # Model options for this batch
        model_options = {
            "inflow_ensemble_indices": indices,
        }
        
        # Build model
        mb = pywrdrb.ModelBuilder(
            inflow_type=f'{inflow_type}',
            start_date=start_date,
            end_date=end_date,
            options=model_options,
        )
        
        # Save model
        model_fname = f"{output_dir}/{inflow_type}_rank{rank}_batch{batch}.json"
        mb.make_model()
        mb.write_model(model_fname)
        
        # Load model
        model = pywrdrb.Model.load(model_fname)
        
        # Get list of parameters for specific results sets
        all_parameter_names = [p.name for p in model.parameters if p.name]
        subset_parameter_names = get_parameter_subset_to_export(
            all_parameter_names, 
            results_set_subset=SAVE_RESULTS_SETS
        )
        export_parameters = [p for p in model.parameters if p.name in subset_parameter_names]
        
        # Setup output recorder
        batch_output_filename = f"{output_dir}/{inflow_type}_rank{rank}_batch{batch}.hdf5"
        recorder = pywrdrb.OutputRecorder(
            model=model,
            output_filename=batch_output_filename,
            parameters=export_parameters
        )
        
        # Run simulation
        model.run()
        
        batch_filenames.append(batch_output_filename)
    
    # Wait for all ranks to complete their batches
    comm.Barrier()
    
    # Combine all batched outputs for this ensemble set
    if rank == 0:
        print(f'Combining batched outputs...')
        
        # Find all batch files for this set
        batch_pattern = f"{output_dir}/{inflow_type}_rank*_batch*.hdf5"
        all_batch_files = glob.glob(batch_pattern)
                
        print(f"Found {len(all_batch_files)} batch files to combine")
                
        # Combine batch files
        combine_batched_hdf5_outputs(all_batch_files, output_filename)
        print("Successfully ran the simulation with custom inflow type")

    # Clear old batched output files if they exist
    if rank == 0:
        print(f"Cleaning up old batch files...")
        batch_pattern = f"{output_dir}/{inflow_type}_rank*_batch*.hdf5"
        model_pattern = f"{output_dir}/{inflow_type}_rank*_batch*.json"
        
        for pattern in [batch_pattern, model_pattern]:
            old_files = glob.glob(pattern)
            for file in old_files:
                if os.path.exists(file):
                    os.remove(file)
        print('Done!')
    
    comm.Barrier()  # Wait for cleanup
    
