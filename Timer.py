import datetime


class Timer:
    
    def __init__(self, name, level):
        self.name = name
        self.level = int(level)
        self.start()
    
    def indent(self):
        for _ in range(self.level):
            print('--', end='')
        if self.level != 0:
            print(' ', end='')
    
    def start(self):
        self.ts = datetime.datetime.now()
        print(self.ts.strftime('%T'), end=' ')
        self.indent()
        print(self.name)
    
    def end(self):
        self.te = datetime.datetime.now()
        print(self.te.strftime('%T'), end=' ')
        self.indent()
        duration = str(self.te-self.ts).split('.')[0]
        print(f'Done in {duration}')