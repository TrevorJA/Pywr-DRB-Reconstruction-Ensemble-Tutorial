"""
Contains configuration settings for the Pywr-DRB Reconstruction Ensemble Tutorial.
"""
### Dataset settings
inflow_type = 'obs_pub_nhmv10_BC_ObsScaled_ensemble'

start_date = '1945-01-01'
end_date = '2023-12-31'

### Files and folders
input_dir = f'./pywrdrb_inputs/{inflow_type}'
gage_flow_filename = f'{input_dir}/gage_flow_mgd.hdf5'
catchment_inflow_filename = f"{input_dir}/catchment_inflow_mgd.hdf5"

output_dir = f"./pywrdrb_outputs/"
output_filename = f"{output_dir}/{inflow_type}.hdf5"


### pywrdrb simulation settings

# this controls the max realizations that are run via pywr's internal parallelization
N_REALIZATIONS_PER_PYWRDRB_BATCH = 10

# this controls what model parameters are saved in the output
# this helps reduce size of the output file
SAVE_RESULTS_SETS = [
    "major_flow", 
    "inflow", 
    "res_storage",
    "lower_basin_mrf_contributions", 
    "mrf_target", 
    "ibt_diversions", 
    "ibt_demands",
    "nyc_release_components"
]