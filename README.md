# Pywr-DRB-Reconstruction-Ensemble-Tutorial

## Motivation

We recently posted ["Introducing Pywr-DRB - Part 1"](https://waterprogramming.wordpress.com/2025/07/09/introducing-pywr-drb-part-1/) where we highlighted the latest 2.0 version of our Pywr-DRB model. 

Pywr-DRB is designed to simulate water resource systems operations in the Delaware River Basin (DRB) under different flow conditions. 


This post should be considered an "Advanced" Pywr-DRB tutorial and it assumes that you are already familiar with the basic Pywr-DRB API functionality for single-scenario simulations.

If you would like to get up-to-speed on the Pywr-DRB basics, I'd recommend you begin by exploring the links below. 

**Links:**
- [Pywr-DRB Repository](https://github.com/Pywr-DRB/Pywr-DRB)
	- [Pywr-DRB v2.0.0 Release Notes](https://github.com/Pywr-DRB/Pywr-DRB/releases/tag/v2.0.0)
- [Pywr-DRB Documentation](https://pywr-drb.github.io/Pywr-DRB/intro.html)
***
## Parallel Simulation in Pywr-DRB



### Custom inflow ensemble input files

When running Pywr-DRB with a custom input dataset, you must originally provide full natural streamflow estimates at all Pywr-DRB node locations.  This natural streamflow file must be called `gage_flow_mgd.<filetype>`.

For single-realization datasets, this file must be `csv` type.  

For many-realization, ensemble datasets, this file must be an HDF5 (`.hdf5`) file.

Before we can run the Pywr-DRB simulation, we need to generate some additional input files which are derived from the `gage_flow_mgd` file.  Ultimately, before running an ensemble simulation, we will need:

- `gage_flow_mgd.hdf5`
	- Contains full-natural streamflow timeseries for each node.
- `catchment_inflow_mgd.hdf5`
	- Contains marginal catchment inflow timeseries for each node.  
- `predicted_inflows_mgd.hdf5`
	- Contains 1-4 day ahead predictions of inflows at multiple nodes, which are used to inform the NYC reservoir operations for downstream flow targets. 


### Parallelization of Pywr-DRB Simulations 

Pywr-DRB supports two-levels of parallelization:
1. Intra-core parallelization using MPI
2. Inter-core parallelization using pywr

The first level is the normal MPI type parallelization, where each core is running an independent 


### Post-processing data

By default, Pywr-DRB will save all output data within an `hdf5` file, regardless of whether you are running a single realization or an ensemble. 

We designed the `pywrdrb.Data` object to handle ensemble data by default - when loading an output file, it will automatically retrieve all of the ensemble realizations within the output file. 

After running `data = pywrdrb.Data().load_output()`, the full ensemble will be stored within the `data` object in a hierarchical structure according to:

```
data.<results_set>[<output_name>][<scenario_id>] -> pd.DataFrame
```







***
## DRB Streamflow Reconstruction Ensemble Dataset



**Paper citation:**
>Amestoy, Trevor J. and Reed, Patrick M., Integrated River Basin Assessment Framework Combining Probabilistic Streamflow Reconstruction, Bayesian Bias Correction, and Drought Storyline Analysis. Available at SSRN: https://ssrn.com/abstract=5240633 or http://dx.doi.org/10.2139/ssrn.5240633

**Dataset citation:**
>Amestoy, T., & Reed, P. (2025). Delaware River Basin Probabilistic Daily Streamflow Reconstruction Ensemble and Water Systems Model Data 1945-2023 (Amestoy and Reed, In Review, EMS) (1.0.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.15101164

We have included the *median* realization of the reconstruction ensemble in the latest [Pywr-DRB v2.0.0 release](https://github.com/Pywr-DRB/Pywr-DRB/releases/tag/v2.0.0#:~:text=4.%20Expanded%20number%20of%20pre%2Dpackaged%20streamflow%20scenarios). That dataset (named `"pub_nhmv10_BC_withObsScaled"`) is a single timeseries reflecting the median streamflow conditions across the ensemble realizations and includes the full 1945-2023 period.

Here, however, we want to run simulations using the full 1,000 member reconstruction ensemble. 

In this tutorial, I show the full workflow, from downloading the reconstruction full natural flow dataset through simulation and loading of output data and plotting some example results. 

### Downloading the Reconstruction from Zenodo

```bash
module load python/3.11.5
python -m virtualenv venv
source venv/bin/activate
pip install requests
python download_reconstruction.py
```

The `01_download_reconstruction.py` script is setup to only download the `drb_historic_streamflow_ensemble_data.zip` from Zenodo , which is 10.69GB. This download took ~40 minutes on Hopper, but may be different depending on the internet speed. 

After the download, we need to unzip the downloaded file:

```bash
# Unzip the file
unzip drb_historic_streamflow_ensemble_data.zip
```

The drb_historic_streamflow_ensemble_data.zip file contains two HDF5 files, each containing 1,000 realizations of daily full natural streamflow at 33 Pywr-DRB node locations across the basin:
- `outputs/ensembles/gage_flow_obs_pub_nhmv10_BC_ObsScaled_ensemble.hdf5`
- `outputs/ensembles/gage_flow_obs_pub_nhmv10_ObsScaled_ensemble.hdf5`


Install Pywr-DRB in your environment.

```
pip install pywr==1.27.4 scipy==
pip install git+https://github.com/Pywr-DRB/Pywr-DRB.git
```

### Using the reconstruction ensemble as input to Pywr-DRB

On the Pywr-DRB docs site, we have a more detailed [Tutorial for 'Using Customized Data to Run Pywr-DRB'](https://pywr-drb.github.io/Pywr-DRB/examples/Tutorial%2004%20Using%20Customized%20Data%20To%20Run%20Model.html) however that tutorial is focused on single-realization datasets. 

The single-realization and ensemble workflows is very similar, however instead of having csv files with a single realization of daily streamflows, the ensemble workflow relies on HDF5 files containing ensembles of streamflow scenarios. 


Create the `pywrdrb_inputs/` directory structure that we want. 

```
mkdir -p pywrdrb_inputs/obs_pub_nhmv10_BC_ObsScaled_ensemble
```

Move and rename gage_flow file to the new dataset directory.

```bash
mv outputs/ensembles/gage_flow_obs_pub_nhmv10_BC_ObsScaled_ensemble.hdf5 pywrdrb_inputs/obs_pub_nhmv10_BC_ObsScaled_ensemble/gage_flow_mgd.hdf5

# delete the empty outputs/ensembles/ folder
rm -r outputs
```


#### Calculating marginal catchment inflows

Pywr-DRB requires "catchment inflows" rather than total streamflows—that is, the local runoff contribution from each sub-basin rather than the cumulative flow from all upstream areas. The script `02_calculate_catchment_inflow.py` performs this conversion for each realization individually:

```python
from pywrdrb.pre.flows import _subtract_upstream_catchment_inflows

inflow_ensemble = {}
for realization in gage_flow_ensemble:
    flows_i = gage_flow_ensemble[realization].copy()
    inflow_ensemble[realization] = _subtract_upstream_catchment_inflows(flows_i)
```

Where the `_subtract_upstream_catchment_inflows` function iteratively subtracts upstream gage flows from downstream totals, working through the node network to separate the local catchment contribution at each location. The result is a set of catchment inflows that can drive the Pywr-DRB simulation without double-counting upstream flows.


#### Predicting inflows at downstream nodes

To simulate realistic operational decisions, Pywr-DRB generates 1-4 day ahead inflow predictions using autoregressive models. The script `03_predict_inflows.py` uses uses the `pywrdrb.pre.PredictedInflowEnsemblePreprocessor()` to generate the ensemble of predicted inflows in parallel using MPI.


```python
from pywrdrb.pre import PredictedInflowEnsemblePreprocessor

# Generate predictions using AR models
preprocessor = PredictedInflowEnsemblePreprocessor(
    flow_type=inflow_type,
    ensemble_hdf5_file=catchment_inflow_filename,
    realization_ids=realization_ids,
    use_mpi=True   # Uses MPI to parallelize
)

preprocessor.load()
preprocessor.process()        
preprocessor.save()
```

After running this script, you will notice that a new file (`predicted_inflows_mgd.hdf5`) appears in the `pywrdrb_inputs/` folder. 


#### Running the simulation



## Conclusions