import numpy as np
import h5py
import os
import pickle
import time

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

def full_first_calculation(dataset, file_path):
    update_first_dataset = first_calculation(dataset)
    update_two_first_dataset = second_calculation(update_first_dataset)
    with open(file_path + "first_full_calc.pickle", "wb") as first_pickle:
            pickle.dump(update_two_first_dataset, first_pickle)
    return update_two_first_dataset
    
def full_second_calculation(dataset, file_path):
    update_second_dataset = third_calculation(dataset)
    update_two_second_dataset = fourth_calculation(update_second_dataset)
    with open(file_path + "second_full_calc.pickle", "wb") as second_pickle:
            pickle.dump(update_two_second_dataset, second_pickle)
    return update_two_second_dataset

example_pickle_file_path = "/home/zvladimi/work_with_large_data_boilerplate/"
example_hdf5_data_file_path = "/home/zvladimi/work_with_large_data_boilerplate/all_datasets.hdf5"
example_hdf5_save_file_path = "/home/zvladimi/work_with_large_data_boilerplate/calculations.hdf5"


np.random.seed(11)
first_dataset = np.random.rand(200000, 1000)
second_dataset = np.random.rand(50000, 250)

def calculation_from_pickle(file_path, first_dataset, second_dataset):
    if check_pickle_exist(file_path + "first_full_calc.pickle") and not check_pickle_exist(file_path + "second_full_calc.pickle"):
        with open(file_path + "first_full_calc.pickle", "rb") as first_pickle:
            first_calc = pickle.load(first_pickle)
        return first_calc, full_second_calculation(second_dataset, file_path)
    
    elif not check_pickle_exist(file_path + "first_full_calc.pickle") and check_pickle_exist(file_path + "second_full_calc.pickle"):
        with open(file_path + "second_full_calc.pickle", "rb") as second_pickle:
            second_calc = pickle.load(second_pickle)
        return full_first_calculation(first_dataset, file_path), second_calc
            
    elif check_pickle_exist(file_path + "first_full_calc.pickle") and check_pickle_exist(file_path + "second_full_calc.pickle"):
        with open(file_path + "first_full_calc.pickle", "rb") as first_pickle:
            first_calc = pickle.load(first_pickle)
        with open(file_path + "second_full_calc.pickle", "rb") as second_pickle:
            second_calc = pickle.load(second_pickle)
        return first_calc, second_calc
    
    else:
        return full_first_calculation(first_dataset, file_path), full_second_calculation(second_dataset, file_path)


with h5py.File(example_hdf5_data_file_path, "a") as hdf5_file: 
    # hdf5_file.create_dataset("first_dataset", maxshape=(None, None), data = first_dataset, chunks = True) 
    # hdf5_file.create_dataset("second_dataset", maxshape=(None, None), data = second_dataset, chunks = True)
    first_dataset = hdf5_file['first_dataset']
    second_dataset = hdf5_file['second_dataset']  
    
first_full_calc, second_full_calc = calculation_from_pickle(example_pickle_file_path, first_dataset, second_dataset)
    
with h5py.File(example_hdf5_save_file_path, "a") as hdf5_file: 
    if "final_calc_1" in hdf5_file.keys():
        print("hi")
        del hdf5_file['final_calc_1']
    if "final_calc_2" in hdf5_file.keys():
        del hdf5_file['final_calc_2']
    hdf5_file.create_dataset("final_calc_1", maxshape=(None, None), data = first_full_calc, chunks = True) 
    hdf5_file.create_dataset("final_calc_2", maxshape=(None, None), data = second_full_calc, chunks = True)
    
    
def calculation_from_hdf5(file_path):
    if check_hdf5_exist(file_path):
        t1 = time.time()
        print("second")
        with h5py.File(example_hdf5_file_path, "a") as hdf5_file:        
            for i,key in enumerate(hdf5_file.keys()):
                if hdf5_file[key].shape != first_dataset.shape and i == 0:
                    del hdf5_file[key]
                    print(key)(first_dataset, hdf5_file)
                    hdf5_file.create_dataset("final_calc_1", maxshape=(None, None), data = full_first_calculation(first_dataset), chunks = True) 
                elif hdf5_file[key].shape != second_dataset.shape and i == 1:
                    del hdf5_file[key]
                    full_second_calculation(second_dataset, hdf5_file)
                    hdf5_file.create_dataset("final_calc_2", maxshape=(None, None), data = full_second_calculation(second_dataset), chunks = True)
                else:
                    print("Size is the same so no change made")
        t2 = time.time()
        print("Time taken:", t2 - t1, "Seconds")
    else:
        print("first")
        t1 = time.time()
        with h5py.File(example_hdf5_file_path, "a") as hdf5_file:
            full_first_calculation(first_dataset, hdf5_file)
            hdf5_file.create_dataset("final_calc_1", maxshape=(None, None), data = full_first_calculation, chunks = True) 
            full_second_calculation(second_dataset, hdf5_file)
            hdf5_file.create_dataset("final_calc_2", maxshape=(None, None), data = full_second_calculation(second_dataset), chunks = True)
        t2 = time.time()
        print("Time taken:", t2 - t1, "Seconds")
        
        
t1 = time.time()

# def full_first_calculation(dataset):
#     update_first_dataset = first_calculation(dataset)
#     update_two_first_dataset = second_calculation(update_first_dataset)
    
#     return update_two_first_dataset
    
# def full_second_calculation(dataset):
#     update_second_dataset = third_calculation(dataset)
#     update_two_second_dataset = fourth_calculation(update_second_dataset)
#     return update_two_second_dataset
# first_calc = full_first_calculation(first_dataset)
# second_calc = full_second_calculation(second_dataset)
t2 = time.time()
# print("Time taken:", t2 - t1, "Seconds")       
 
t1 = time.time()
first_calc, second_calc = calculation_from_pickle(example_pickle_file_path, first_dataset, second_dataset) 
t2 = time.time()
print("Time taken:", t2 - t1, "Seconds")             