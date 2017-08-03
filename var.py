import pickle

def load(obj):
    with open(obj) as f: 
        filename = pickle.load(f)
    return filename
def save(filename, save_obj):
    with open(filename + '.pickle', 'w') as f: 
        pickle.dump(save_obj, f)
<<<<<<< HEAD
=======

>>>>>>> 0d3b0a3b14008bea790d06a6fb930e5deb83b4eb
