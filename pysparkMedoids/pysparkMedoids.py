#!/usr/bin/python

from scipy.spatial import distance
from parallelkMedoids import parallelkMedoids
from kMedoids import kMedoids

class pysparkMedoids(object):

    def __init__(self,
                data,
                sc,
                k=2,
                dist_func=distance.euclidean,
                sample_size=100,
                max_iters=10,
                resample_interval=2,
                verbose=True,
                parallel_version=True):

        if parallel_version:
            self.km = parallelkMedoids(data,
                                       sc,
                                       k,
                                       dist_func,
                                       sample_size,
                                       max_iters,
                                       resample_interval,
                                       verbose)
        else:
            self.km = kMedoids(data,
                               sc,
                               k,
                               dist_func,
                               sample_size,
                               max_iters,
                               resample_interval,
                               verbose)

    def run(self):
        sol = self.km.run()
        return sol
