#!/usr/bin/env python3

"""
Copyright (c) 2019 Argonne National Laboratory

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import math as m
import cmath as cm
import itertools as i
import timeit as t
import matplotlib.pyplot as plt
import socket, os
from pyspark import SparkContext, SparkConf

plt.rcParams["figure.figsize"] = 15,12

# Save figures under
figures_dir = 'figures/'

def ppp(file, p, nid = -1, fmt = 'r,'):
    "Plot complex points, with an optional id number."
    lp = len(p)
    x,y = zip(*map(lambda x: (x.real,x.imag), p))
    plt.plot(x,y,fmt)
    if nid < 0:
        plt.title("{} points".format(lp))
    else:
        plt.title("#{} with {} points".format(nid,lp))
    if file != '':
        plt.savefig(figures_dir + file)
        plt.close()

# Julia set with $f(z) = z^2 -0.512511498387847167 + 0.521295573094847167i$
raster_prec = 512 # Snap points to a grid of this size, see `rasterize`
def julia_inverse(p):
    "Inverse iteration of the chosen Julia set function."
    c = -0.512511498387847167 + 0.521295573094847167j
    z = cm.sqrt(p-c)
    return [z, -z]
def rasterize(p):
    "Snap points to a grid with number of grid points given by `raster_prec`."
    x, y = p.real, p.imag
    x = round(x*raster_prec)/raster_prec
    y = round(y*raster_prec)/raster_prec
    return x + y*1j

# The plastic number, $\rho$, is the unique real solution to
# the cubic equation $x^3 = x + 1$.
# We use it for generating a quasi-random sequence in 2D for
# the initial set of complex numbers later.
rho = ((9+m.sqrt(69))/18)**(1/3)+((9-m.sqrt(69))/18)**(1/3)

# Obtain the Spark Context
sc = SparkContext(conf=SparkConf())
print(sc)

# Print configurations.
print(sc.getConf().getAll())

# Create an RDD with 64 elements.
n_rdd = sc.range(1,2**6+1)

# Print how the RDD gets mapped.
print(n_rdd.map(lambda x: (socket.gethostname(), os.getppid(), os.getpid())).distinct().collect())

# Mark for cache() in memory
points_rdd = n_rdd.map(lambda n: (n/rho %1)/10 + (n/(rho*rho) %1)/10*1j).cache()

# Print the total number of partitions
print(points_rdd.getNumPartitions())

# Print the number of elements in each partition
print(points_rdd.glom().map(len).collect())

# Plot the initial points, the quasi-random series.
ppp('plot_init.pdf', points_rdd.collect())

# Plot the first iteration.
ppp('plot_first.pdf', points_rdd.flatMap(julia_inverse, True).map(rasterize, True).distinct().collect())

def iterate_rdd(ps):
    "Perform one inverse iteration on the RDD, ps."
    # We need repartition() after distinct() for balancing parallel workload.
    return ps.flatMap(julia_inverse, True).map(rasterize, True).distinct().repartition(ps.getNumPartitions())

# Plot the results of two iterations.
ppp('plot_second.pdf', iterate_rdd(iterate_rdd(points_rdd)).collect())

def iterateN(n, iterfunc, ps):
    "Iterate `iterfunc` on ps by `N` times."
    p = ps
    for i in range(n):
        p = iterfunc(p)
    return p

def iterateP_rdd(n, iterfunc, ps):
    "Plot results of the first n iterations, counting from 0."
    r = m.ceil(m.sqrt(n))
    c = m.ceil(n/r)
    p = ps
    #plt.figure(figsize=(15,12))
    for i in range(n):
        plt.subplot(r,c,i+1)
        ppp('', p.collect(), i)
        if i == n-1: break
        p = iterfunc(p).cache()
    plt.savefig(figures_dir + 'plot_first_{}.pdf'.format(n))
    plt.close()

# Plot the results of first 9 iterations, counting from 0.
iterateP_rdd(9,iterate_rdd,points_rdd)

def refine_rdd(n,ps):
    """
        Iterate on the points, `ps`, until convergence.
        We freeze the points, and shuffle new data points every `n` iterations.
    """
    np = ps.getNumPartitions()
    p = ps
    fp = sc.emptyRDD()
    i = 0
    while True:
        print("iter {}  len(p) = {}".format(i,p.count()))
        #print(f"iter {i}  p {p.glom().map(len).collect()}  fp {fp.glom().map(len).collect()}")
        p = iterateN(n,iterate_rdd,p).subtract(fp).cache()
        if p.isEmpty(): break
        p = p.repartition(min(np,m.ceil(p.count()/10))).cache()  # Try removing the repartition call here and below.
        fp = fp.union(p).repartition(np).cache()
        i = i+1
    return fp

# Plot the end results.
ppp('plot_julia.pdf', refine_rdd(8,points_rdd).collect())
