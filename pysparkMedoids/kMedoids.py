#!/usr/bin/python

from __future__ import division
from scipy.spatial import distance

class kMedoids(object):

    def __init__(self, data, sc, k, dist_func, sample_size,
            max_iters, resample_interval, verbose):

        n = data.count()
        self.data = data
        self.sc = sc
        self.dist_func = dist_func
        self.ss = min(sample_size, n)
        self.fraction = min(1.0, self.ss/n)
        self.max_iters = max_iters
        self.k = k
        self.resample_interval = resample_interval
        self.verbose = verbose

    def run(self):
        # Define auxiliary methods inside run
        # Cannot make them static methods because the object
        # contains reference to the spark context

        def _get_cluster_distance(point, cluster, dist_func):
            dist = reduce(lambda x,y: x + dist_func(point, y), cluster, 0.0)
            return dist

        def _get_cluster_medoid(cluster, dist_func):
            m = map(lambda x: (x, _get_cluster_distance(x, cluster, dist_func)), cluster)
            m = min(m, key=lambda x: x[1])
            return m

        def _get_clusters(data, medoids, dist_func):
            m = (data.map(lambda x: (_get_closest_medoid(x, medoids, dist_func)[0], x))
                     .groupByKey()
                     .map(lambda x: list(x[1])))
            return m

        def _get_closest_medoid(point, medoids, dist_func):
            m = enumerate(medoids)
            m = map(lambda x: (x[0], dist_func(point, x[1])), m)
            medoid = min(m, key=lambda x: x[1])
            return medoid

        # Run method
        data = self.data
        sc = self.sc

        # Create sample and initial set of medoids
        sample = data.sample(fraction=self.fraction, withReplacement=False).take(self.ss)
        sample = sc.parallelize(sample)
        medoids = sample.take(self.k)
        dist_func = self.dist_func

        itr = 1
        tot_dist = (sample.map(lambda x: _get_closest_medoid(x, medoids, dist_func))
                          .map(lambda x: x[1])
                          .reduce(lambda x,y: x + y))

        while itr <= self.max_iters:
            if self.verbose:
                print "Iteration: {}".format(itr)

            # Resample after the resample interval
            if itr > 1 and (itr % self.resample_interval == 0):
                if self.verbose:
                    print "Resampling data"
                sample = data.sample(fraction=self.fraction,
                                     withReplacement=False).take(self.ss)
                sample = sc.parallelize(sample)

            # Get cluster assignment for each point in sample
            clusters = _get_clusters(sample, medoids, dist_func)
            new_medoids = clusters.map(lambda x: _get_cluster_medoid(x, dist_func))
            new_tot_dist = (new_medoids.map(lambda x: x[1])
                                       .fold(0.0, lambda x,y: x + y))
            new_medoids = new_medoids.flatMap(lambda x: [x[0]]).collect()

            if self.verbose:
                print "Old Metric: {}\nNew Metric: {}\n".format(tot_dist, new_tot_dist)

            tot_dist = new_tot_dist
            medoids = new_medoids

            itr += 1

        if self.verbose:
            print "Evaluating final cluster assignment"

        clusters = _get_clusters(data, medoids, dist_func)
        tot_dist = (data.map(lambda x: _get_closest_medoid(x, medoids, dist_func))
                        .map(lambda x: x[1])
                        .reduce(lambda x,y: x + y))

        return clusters, medoids, tot_dist
