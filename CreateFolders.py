import os


def create_folder_structure(project_root):
    f_list = ['output/', 
              'output/rawh/', 'output/splith/', 'output/mergedh/',
              'output/augh/', 'output/slct/', 'output/fmtd/', 
              'output/csvs/', 'output/ready/', 'output/chlog/',
              'output/chlog/oldv/', 'output/chlog/newv/',
              'output/chlog/cmpd/', 'output/chlog/oldv/parts/',
              'output/chlog/oldv/mrgd', 'output/chlog/newv/parts/',
              'output/chlog/newv/mrgd']
    for folder in f_list:
        path = project_root + folder
        if os.path.isdir(path):
            pass
        else:
            os.mkdir(path)
    return None