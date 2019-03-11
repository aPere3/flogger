.. flogger documentation master file, created by
   sphinx-quickstart on Thu Jan 17 16:05:31 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Flogger Documentation
=====================

Flogger is a data logging tool that tries to provide a simple and unified interface to various logging solutions one may
want to use in deep-learning experiments. Put differently, it allows to store various types of data an experiment can
generate (images, curves, text, videos, ...), into various forms (Json, matplotlib figures, tensorboard logs, etc...).
As far as it can go with python, the handling of data can be made asynchronously, to avoid blocking the experiment thread.

.. toctree::
   :maxdepth: 2
   :caption: Table of Content

   tutorial
   remarks
   api


