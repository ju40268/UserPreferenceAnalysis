import pickle

def load(obj):
    with open(obj) as f: 
        filename = pickle.load(f)
    return filename
def save(filename, save_obj):
    with open(filename + '.pickle', 'w') as f: 
        pickle.dump(save_obj, f)

