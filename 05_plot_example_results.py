import pandas as pd
import matplotlib.pyplot as plt

import pywrdrb

from config import inflow_type, output_filename


### Load output data

results_sets = ['res_storage', 'major_flow']

data = pywrdrb.Data(print_status=True)

# Load model output data
data.load_output(
    results_sets=results_sets,
    output_filenames=[output_filename])


# The `data` object now contains data with format:
# data.results_sets[output_filename][<realization_id>] -> pd.DataFrame
# For example: 
# data.res_storage[inflow_type][0]
# will return a DataFrame for the first realization of the `res_storage` results set, 
# containing a column for each reservoir and the date index.

### Plotting

# Create a plot of NYC storage on top and Montague flow on bottom
# For each plot, we will plot the mean, 90% CI across realizations
# First, we need to extract the ensemble data into a single DataFrame

realization_ids = list(data.res_storage[inflow_type].keys())

nyc_reservoirs = ['cannonsville', 'pepacton', 'neversink']
nyc_agg_storage = []
montague_flow = []
for i in realization_ids:
    df = data.res_storage[inflow_type][i].loc[:, nyc_reservoirs].sum(axis=1)
    nyc_agg_storage.append(df)
    montague_flow.append(data.major_flow[inflow_type][i].loc[:, 'delMontague'])

nyc_agg_storage_df = pd.concat(nyc_agg_storage, axis=1)
montague_flow_df = pd.concat(montague_flow, axis=1)

# apply 7-day rolling mean to smooth the data
nyc_agg_storage_df = nyc_agg_storage_df.rolling(window=7).mean()
montague_flow_df = montague_flow_df.rolling(window=7).mean()

# Convert nyc storage to %
max_storage = nyc_agg_storage_df.max().max()
nyc_agg_storage_df = nyc_agg_storage_df / max_storage * 100


plot_start = '2017-10-01'
plot_end = '2023-12-31'

fig, axs = plt.subplots(nrows=2, ncols=1, figsize=(12, 8), sharex=True)

ax = axs[0]
ax.fill_between(
    nyc_agg_storage_df.index,
    nyc_agg_storage_df.quantile(0.1, axis=1),
    nyc_agg_storage_df.quantile(0.9, axis=1),
    color='orange', alpha=0.5, label='Simulated Ensemble 90% CI')

ax.plot(nyc_agg_storage_df.mean(axis=1), color='orange', label='Simulated Ensemble Mean')
ax.set_ylabel('Combined NYC Reservoir Storage (%)')
ax.set_ylim(0, 100)

ax = axs[1]
ax.fill_between(
    montague_flow_df.index,
    montague_flow_df.quantile(0.1, axis=1),
    montague_flow_df.quantile(0.9, axis=1),
    color='orange', alpha=0.5, label='Simulated Ensemble 90% CI')

ax.plot(montague_flow_df.mean(axis=1), color='orange', label='Simulated Ensemble Mean')
ax.set_ylabel('Montague Flow (MGD)')
ax.set_ylim(0.1, 40000)

ax.set_xlabel('Date')
ax.set_xlim(pd.to_datetime(plot_start), pd.to_datetime(plot_end))

ax.legend()
plt.tight_layout()
plt.savefig(f'nyc_storage_and_montague_flow_{inflow_type}_results.png', dpi=300)



