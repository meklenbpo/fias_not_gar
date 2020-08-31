import os


def scan_a_folder(folder:str, prefix='', abspth=False):
    """Take a folder name, get a list of files, apply a filter
    and return as a list.
    """
    curdir = os.getcwd()
    os.chdir(folder)
    
    file_list = os.listdir('.')
    if '.DS_Store' in file_list:
        file_list.remove('.DS_Store')
    new_list = [x for x in file_list if x.startswith(prefix)]
    
    if abspth:
        new_list = [os.path.abspath(x) for x in new_list]
    
    os.chdir(curdir)
    return new_list