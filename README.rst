link.parallel
=============

**link.parallel** is a database agnostic query system.

See documentation_ for more informations.

.. _documentation: https://linkparallel.readthedocs.io

.. image:: https://img.shields.io/pypi/l/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: License

.. image:: https://img.shields.io/pypi/status/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: Development Status

.. image:: https://img.shields.io/pypi/v/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: Latest release

.. image:: https://img.shields.io/pypi/pyversions/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/implementation/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: Supported Python implementations

.. image:: https://img.shields.io/pypi/wheel/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel
   :alt: Download format

.. image:: https://travis-ci.org/linkdd/link.parallel.svg?branch=master&style=flat-square
   :target: https://travis-ci.org/linkdd/link.parallel
   :alt: Build status

.. image:: https://coveralls.io/repos/github/linkdd/link.parallel/badge.svg?style=flat-square
   :target: https://coveralls.io/r/linkdd/link.parallel
   :alt: Code test coverage

.. image:: https://img.shields.io/pypi/dm/link.parallel.svg?style=flat-square
   :target: https://pypi.python.org/pypi/link.parallel/
   :alt: Downloads

.. image:: https://landscape.io/github/linkdd/link.parallel/master/landscape.svg?style=flat-square
   :target: https://landscape.io/github/linkdd/link.parallel/master
   :alt: Code Health

.. image:: https://www.quantifiedcode.com/api/v1/project/4cc042fb351e44228a67fc7d6835ae6b/badge.svg
  :target: https://www.quantifiedcode.com/app/project/4cc042fb351e44228a67fc7d6835ae6b
  :alt: Code issues

Installation
------------

.. code-block:: text

   pip install link.parallel

Features
--------

 * parallel loops interface with IPython and multiprocessing drivers
 * Map/Reduce middleware

Examples
--------

Create your map/reduce functions:

.. code-block:: python

   from b3j0f.task import register_task


   @register_task('mymap')
   def mymap(mapper, item):
       if item['i'] < 5:
           mapper.emit('i_lt_5', item)

       elif item['i'] > 5:
           mapper.emit('i_gt_5', item)

       else:
           mapper.emit('i_eq_5', item)


   @register_task('myreduce')
   def myreduce(key, values):
       return (key, len(values))


Get input data and pass it to the middleware:

.. code-block:: python

   from link.middleware.core import Middleware

   mapreduce = Middleware.get_middleware_by_uri(
       'mapreduce+ipython:///test/classify?mapcb=mymap&reducecb=myreduce'
   )

   items = # load items
   result = dict(mapreduce(items))

   print(result)

Donating
--------

.. image:: https://cdn.rawgit.com/gratipay/gratipay-badge/2.3.0/dist/gratipay.svg
   :target: https://gratipay.com/~linkdd/
   :alt: Support via Gratipay
