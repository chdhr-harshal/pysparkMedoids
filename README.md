# pysparkMedoids

This package provides a highly parallelized version of the popular kMedoids clustering algorithm implemented using pySpark.

There are two versions in this package:
1. kMedoids using pySpark (parallelizes the step of finding medoid within each cluster) (pass the parameter "parallel_version=False").
2. Highly parallel version that uses combination of multiprocessing.pool.Threadpool and pySpark to parallelize every step. (Default behavior).

Due to nature of kMedoids, it is computationally heavy  because it evaluates the O(n^2) distance matrix for every cluster. In
this implementation, we sample a user provided number of points after every resample_interval.


### Installation

~~~~
>> git clone https://github.com/chdhr-harshal/pysparkMedoids.git
>> cd pysparkMedoids
>> python setup.py install
~~~~

### Usage
~~~~
>> from pysparkMedoids import pysparkMedoids
>> pkm = pysparkMedoids.pysparkMedoids(data=rdd_data,             # Rdd containing datapoints
                                       sc=sc,                     # Spark context 
                                       k=2,                       # Number of clusters
                                       dist_func=dist_func,       # User defined distance function
                                       sample_size=1000,          # Number of samples during resampling
                                       max_iters=10,              # Number of iterations
                                       resample_interval=2,       # Resampling after x iterations
                                       verbose=True,              # Verbose 
                                       parallel_version=True      # Use highly parallel version
                                   )

>> sol = pkm.run()

>> clusters = sol[0].collect() # Collect operation outputs list of clusters
>> medoids = sol[1] # List of medoids
>> tot_dist = sol[2] # Total distance metric of clustering using the above medoids
~~~~
