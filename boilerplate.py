import numpy as np
import h5py
import os
import pickle

def check_pickle_exist(file_path):
    if os.path.isfile(file_path):
        return True
    else:
        return False

def check_hdf5_exist(file_path):
    if os.path.isfile(file_path):
        return True
    else:
        return False
    
def check_shape_match(key_name, dataset_shape, file):
    if file[key_name].shape == dataset_shape:
        return True
    else:
        return False

def first_calculation(dataset, file_path):
    update_dataset = dataset * 5
    with open(file_path, "wb") as pickle_file:
        pickle.dump(update_dataset, pickle_file)
    return update_dataset

def second_calculation(dataset):
    update_dataset = dataset ** 4
    return update_dataset

def third_calculation(dataset):
    updated_dataset = (dataset/54) ** 5
    return updated_dataset

def fourth_calculation(dataset):
    updated_dataset = np.sqrt(dataset) + 33.2
    return updated_dataset

example_pickle_file_path = "/home/zvladimi/work_with_large_data/dataset.pickle"
example_hdf5_file_path = "/home/zvladimi/work_with_large_data/all_datasets.hdf5"


np.random.seed(11)
first_dataset = np.random.rand(100000, 100)
second_dataset = np.random.rand(50000, 250)

if check_hdf5_exist(example_hdf5_file_path):
    with h5py.File(example_hdf5_file_path, "a") as hdf5_file:
        for i, key in enumerate(hdf5_file.keys()):
            if i == 0 or i == 1:
                if check_shape_match(key, first_dataset.shape, hdf5_file):
                    
            else:
                check_shape_match(key, second_dataset.shape, hdf5_file)
            
else:
    with h5py.File(example_hdf5_file_path, "a") as hdf5_file:
        update_first_dataset = first_calculation(first_dataset)
        hdf5_file["first_calc"] = update_first_dataset
        update_two_first_dataset = second_calculation(update_first_dataset)
        hdf5_file["second_calc"] = update_two_first_dataset
        
        update_second_dataset = third_calculation(second_dataset)
        hdf5_file["third_calc"] = update_second_dataset
        update_two_second_dataset = fourth_calculation(update_second_dataset)
        hdf5_file["fourth_calc"] = update_two_second_dataset