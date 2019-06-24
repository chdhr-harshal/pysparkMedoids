#!/usr/bin/python

from __future__ import division
from multiprocessing.pool import ThreadPool

class parallelkMedoids(object):

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

    def _get_closest_medoid(self, data, medoids):
        dist_func = self.dist_func
        m = (data.cartesian(medoids)
                 .map(lambda x: (x[0][1], (x[1][1], dist_func(x[0][0], x[1][0]))))
                 .reduceByKey(lambda x,y: (x[0], x[1]) if x[1]<=y[1] else (y[0], y[1])))
        m = data.map(lambda x: (x[1], x[0])).join(m)
        return m

    def _get_cluster_medoid(self, cluster):
        cluster = cluster.zipWithIndex()
        dist_func = self.dist_func
        m = (cluster.cartesian(cluster)
                    .map(lambda x: (x[0][1], (dist_func(x[0][0], x[1][0]), x[0][0])))
                    .reduceByKey(lambda x,y: (x[0] + y[0], x[1]))
                    .min(lambda x: x[1][0])[1][1])
        return m

    @staticmethod
    def _get_tot_dist(medoid_assignment):
        tot_dist = (medoid_assignment.map(lambda x: x[1][1][1])
                                     .reduce(lambda x,y: x + y))
        return tot_dist

    @staticmethod
    def _get_clusters(medoid_assignment):
        clusters = (medoid_assignment.groupBy(lambda x: x[1][1][0])
                                     .map(lambda x: [y[1][0] for y in x[1]]))
        return clusters

    def run(self):
        data = self.data
        sc = self.sc
        pool = ThreadPool(self.k)

        # Create sample and initial set of medoids
        sample = data.sample(fraction=self.fraction, withReplacement=False).take(self.ss)
        sample = sc.parallelize(sample)
        medoids = sample.take(self.k)

        sample = sample.zipWithIndex()
        medoids = sc.parallelize(medoids).zipWithIndex()

        itr = 1

        # Get initial medoid assignment for each point in sample
        medoid_assignment = self._get_closest_medoid(sample, medoids)
        tot_dist = self._get_tot_dist(medoid_assignment)

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
                sample = sample.zipWithIndex()

            # Get cluster assignment for each point in sample
            clusters = self._get_clusters(medoid_assignment)
            num_clusters = clusters.cache().count()

            if self.verbose:
                print "Created {} clusters".format(num_clusters)

            # Find updated medoids
            clusters_rdd = [sc.parallelize(x) for x in clusters.collect()]
            get_cluster_medoid = self._get_cluster_medoid
            new_medoids = pool.map(get_cluster_medoid, clusters_rdd)

            # If empty clusters were created, add new random sample medoids
            if len(new_medoids) < self.k:
                print "Adding {} new medoids".format(k - len(new_medoids))
                extra_medoids = data.sample(fraction=0.01, withReplacement=False).take(k - len(new_medoids))
                new_medoids += extra_medoids

            new_medoids = sc.parallelize(new_medoids).zipWithIndex()
            new_medoid_assignment = self._get_closest_medoid(sample, new_medoids)
            new_tot_dist = self._get_tot_dist(new_medoid_assignment)

            if self.verbose:
                print "Old Metric: {}\nNew Metric: {}\n".format(tot_dist, new_tot_dist)

            tot_dist = new_tot_dist
            medoids = new_medoids
            medoid_assignment = new_medoid_assignment

            itr += 1

        if self.verbose:
            print "Evaluating final cluster assignment"

        medoid_assignment = self._get_closest_medoid(data.zipWithIndex(), medoids)
        clusters = self._get_clusters(medoid_assignment)
        medoids = medoids.map(lambda x: x[0]).collect()
        tot_dist = self._get_tot_dist(medoid_assignment)
        pool.close()

        return clusters, medoids, tot_dist
