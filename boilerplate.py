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
    

def first_calculation(dataset):
    update_dataset = dataset * 5
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

def full_first_calculation(dataset, file):
    update_first_dataset = first_calculation(dataset)
    update_two_first_dataset = second_calculation(update_first_dataset)
    file.create_dataset("final_calc_1", maxshape=(None, None), data = update_two_first_dataset, chunks = True) 
    
def full_second_calculation(dataset, file):
    update_second_dataset = third_calculation(dataset)
    update_two_second_dataset = fourth_calculation(update_second_dataset)
    file.create_dataset("final_calc_2", maxshape=(None, None), data = update_two_second_dataset, chunks = True)

example_pickle_file_path = "/home/zvladimi/work_with_large_data_boilerplate/dataset.pickle"
example_hdf5_file_path = "/home/zvladimi/work_with_large_data_boilerplate/all_datasets.hdf5"


np.random.seed(11)
first_dataset = np.random.rand(100000, 100)
second_dataset = np.random.rand(50000, 250)

if check_hdf5_exist(example_hdf5_file_path):
    print("second")
    with h5py.File(example_hdf5_file_path, "a") as hdf5_file:        
        for i,key in enumerate(hdf5_file.keys()):
            if hdf5_file[key].shape != first_dataset.shape and i == 1:
                del hdf5_file[key]
                print(key)
                full_first_calculation(first_dataset, hdf5_file)
            elif hdf5_file[key].shape != second_dataset.shape and i == 2:
                del hdf5_file[key]
                full_second_calculation(second_dataset, hdf5_file)
else:
    print("first")
    with h5py.File(example_hdf5_file_path, "a") as hdf5_file:
        full_first_calculation(first_dataset, hdf5_file)
        
        full_second_calculation(second_dataset, hdf5_file)
        
        
        