import os
import sys
from mpi4py import MPI

import pywrdrb
from pywrdrb.utils.hdf5 import get_hdf5_realization_numbers
from pywrdrb.pre import PredictedInflowEnsemblePreprocessor

from config import inflow_type, input_dir, gage_flow_filename, catchment_inflow_filename

# Setup pathnavigator for this specific ensemble set
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
    
    if rank == 0:
        print(f"Preparing Pywr-DRB inputs for {n_realizations} of {inflow_type} ensemble")

        ### Load gage flow ensemble ##########################################

        # Open hdf5 to get realization IDs
        realization_ids = get_hdf5_realization_numbers(gage_flow_filename)
        
        print(f"Found {len(realization_ids)} realizations in gage flow ensemble, using the first {n_realizations} realizations")
        
        # keep only the first n_realizations
        assert n_realizations <= len(realization_ids), \
            f"Requested {n_realizations} realizations, but only found {len(realization_ids)} realizations in {gage_flow_filename}"

        realization_ids = realization_ids[:n_realizations]
    else:
        realization_ids = None
    realization_ids = comm.bcast(realization_ids, root=0)
    

    ### Predicted inflow ensemble ############################################

    # Why? Pywr-DRB uses 1-4 day ahead predicted inflows
    # to decide how much water to release for Montague and Trenton flow targets.
    # We generate these predictions before simulation, to save time later. 
    # Predictions are generated using an AR model. 

    preprocessor = PredictedInflowEnsemblePreprocessor(
                flow_type=inflow_type,
                ensemble_hdf5_file=catchment_inflow_filename,
                realization_ids=realization_ids,
                use_mpi=True
            )

    preprocessor.load()
    preprocessor.process()        
    preprocessor.save()