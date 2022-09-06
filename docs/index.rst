.. Kedro Multinode documentation master file, created by
   sphinx-quickstart on Wed Jul 27 13:25:35 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
===========================================
Kedro Plugin focused on extending and helping the user to process projetaai
data.

Installation
==================

Execute this command in your terminal:

.. code-block:: bash

   pip install kedro-projetaai

Contents
==================

Step Parallelism
******************

.. autosummary::
   kedro_projetaai.pipeline.multinode.multinode
   kedro_projetaai.pipeline.multinode.multipipeline

DataSets
******************

.. autosummary::
   kedro_projetaai.extras.datasets.concatenated_dataset.ConcatenatedDataSet
   kedro_projetaai.extras.datasets.concatenated_dataset.PandasConcatenatedDataSet

Decorators
******************

.. autosummary::
   kedro_projetaai.pipeline.decorators.concat_partitions
   kedro_projetaai.pipeline.decorators.split_into_partitions
   kedro_projetaai.pipeline.decorators.list_output

Helpers
******************

Helpers are filter functions generated according to a specification.
They can be used in multiple string filter scenarios, such as glob filtering.
These functions are designed to be used with the decorators above, but can be
used in other scenarios if needed.

.. autosummary::
   kedro_projetaai.pipeline.decorators.helper_factory.date_range_filter
   kedro_projetaai.pipeline.decorators.helper_factory.regex_filter
   kedro_projetaai.pipeline.decorators.helper_factory.not_filter

Utils
******************
There are more relevant utilitary functions, but only the most important ones
are listed below:

.. autosummary::
   kedro_projetaai.utils.string.UPath

API Reference
==================

* :ref:`modindex`
* :ref:`genindex`

Credits
==================
.. _@gabrieldaiha: https://github.com/gabrieldaiha
.. _@nickolasrm: https://github.com/nickolasrm

This package was created by:

* Gabriel Daiha Alves `@gabrieldaiha`_
* Nickolas da Rocha Machado `@nickolasrm`_
