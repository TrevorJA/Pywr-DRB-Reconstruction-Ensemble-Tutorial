import os
import sys
import pandas as pd

import pywrdrb
from pywrdrb.utils.hdf5 import get_hdf5_realization_numbers, extract_realization_from_hdf5
from pywrdrb.pre.flows import _subtract_upstream_catchment_inflows

from utils import export_ensemble_to_hdf5
from config import inflow_type, input_dir, gage_flow_filename, start_date


# Setup pathnavigator for this specific ensemble set
pn_config = pywrdrb.get_pn_config()
pn_config[f"flows/{inflow_type}"] = os.path.abspath(input_dir)
pywrdrb.load_pn_config(pn_config)

if __name__ == "__main__":

    # Get the number of realizations from the sys.arg inputs
    n_realizations = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    print(f"Preparing Pywr-DRB inputs for {n_realizations} of {inflow_type} ensemble")

    ### Load gage flow ensemble ##########################################

    # Open hdf5 to get realization IDs
    realization_ids = get_hdf5_realization_numbers(gage_flow_filename)
    
    print(f"Found {len(realization_ids)} realizations in gage flow ensemble, using the first {n_realizations} realizations")
    
    # keep only the first n_realizations
    assert n_realizations <= len(realization_ids), \
        f"Requested {n_realizations} realizations, but only found {len(realization_ids)} realizations in {gage_flow_filename}"

    realization_ids = realization_ids[:n_realizations]

    # Load realizations
    syn_ensemble = {}
    for i, real in enumerate(realization_ids):
        if i% 50 == 0:
            print(f"Loading realization {i+1}/{len(realization_ids)}")
        
        syn_ensemble[real] = extract_realization_from_hdf5(
            gage_flow_filename, 
            realization=real, 
            stored_by_node=True)
    
    print(f"Loaded gage flow ensemble with {len(syn_ensemble)} realizations")

    
    ### Calculate catchment inflow #######################################
    # this is done by iteratively subtracting upstream node flows

    local_inflow_ensemble = {}
    for real in syn_ensemble:
        syn_ensemble[real]['delTrenton'] = 0.0
        flows_i = syn_ensemble[real].copy()
        flows_i.index = pd.date_range(start=start_date, 
                                        periods=len(flows_i), 
                                        freq='D')    
        local_inflow_ensemble[real] = _subtract_upstream_catchment_inflows(flows_i)
        
        # Drop the datetime columns
        local_inflow_ensemble[real].drop(columns=['datetime'], inplace=True)
        local_inflow_ensemble[real].index.name = 'datetime'

    # Need to re-organize so that the dict contains:
    # {node: pd.DataFrame}
    # where the pd.DataFrame has columns ['0', '1', ..., 'n'] for each realization
    print('Reorganizing data...')
    catchment_inflow_ensemble = {}
    for real, flows in local_inflow_ensemble.items():
        for node, flow in flows.items():
            if node not in catchment_inflow_ensemble:
                catchment_inflow_ensemble[node] = {}
            catchment_inflow_ensemble[node][real] = flow

    # Convert to DataFrame
    for node in catchment_inflow_ensemble:
        catchment_inflow_ensemble[node] = pd.concat(catchment_inflow_ensemble[node], axis=1)
        
    # Save the inflow ensemble to HDF5
    print(f"Saving catchment inflow ensemble to {inflow_type}/catchment_inflow_mgd.hdf5")
    inflow_ensemble_filename = f'./pywrdrb_inputs/{inflow_type}/catchment_inflow_mgd.hdf5'
    export_ensemble_to_hdf5(catchment_inflow_ensemble, inflow_ensemble_filename)