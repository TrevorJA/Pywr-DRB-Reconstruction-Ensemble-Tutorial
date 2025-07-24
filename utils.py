import h5py
import pywrdrb


def get_parameter_subset_to_export(all_parameter_names, results_set_subset):
    output_loader = pywrdrb.load.Output(output_filenames=[]) # empty dataloader to use methods
    keep_keys = []
    for results_set in results_set_subset:
        if results_set == "all":
            continue
        
        keys_subset, _ = output_loader.get_keys_and_column_names_for_results_set(all_parameter_names, 
                                                                                 results_set)        
        keep_keys.extend(keys_subset)
    return keep_keys


def export_ensemble_to_hdf5(dict, 
                            output_file):
    """
    Export a dictionary of ensemble data to an HDF5 file.
    Data is stored in the dictionary as {realization number (int): pd.DataFrame}.
    
    Args:
        dict (dict): A dictionary of ensemble data.
        output_file (str): Full output file path & name to write HDF5.
        
    Returns:
        None    
    """
    
    dict_keys = list(dict.keys())
    N = len(dict)
    T, M = dict[dict_keys[0]].shape
    column_labels = dict[dict_keys[0]].columns.to_list()
    
    with h5py.File(output_file, 'w') as f:
        for key in dict_keys:
            data = dict[key]
            datetime = data.index.astype(str).tolist() #.strftime('%Y-%m-%d').tolist()
            
            grp = f.create_group(key)
                    
            # Store column labels as an attribute
            grp.attrs['column_labels'] = column_labels

            # Create dataset for dates
            grp.create_dataset('date', data=datetime)
            
            # Create datasets for each array subset from the group
            for j in range(M):
                dataset = grp.create_dataset(column_labels[j], 
                                             data=data[column_labels[j]].to_list())
    return