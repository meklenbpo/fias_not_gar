import os

from ScanFolder import scan_a_folder
from Timer import Timer


class Process:
    
    def __init__(self, greeting='', src='', dst=''):
        self._greeting = greeting
        self._subprocs = []
        self._level = 0
        self.src = src
        self.dst = dst
    
    def add_sub(self, process):
        process._level = self._level + 1
        self._subprocs.append(process)
    
    def launch(self):
        proc_timer = Timer(self._greeting, level=self._level)
        # run useful load if there are no subprocesses
        if len(self._subprocs) == 0:
            for x in self.action():
                if x[0] == 'start':
                    pt = Timer(x[1], level = self._level+1)
                if x == 'end':
                    pt.end()
        # else run all subprocesses
        else:
            for subp in self._subprocs:
                subp.launch()
        proc_timer.end()
    
    def action(self):
        """Useful action. Implemented in children classes.
        Must yield progress information to `launch` method."""
        yield None





