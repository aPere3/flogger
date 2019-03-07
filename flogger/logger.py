#!/usr/bin/env python
# coding: utf-8
"""
This module contains a data logging facility, to store various types of data under various forms, during an experiment.
As far as it can get with python, it provides an asynchronous handling of logging, which allows to avoid stopping the
experiment for logs.
"""
###########
# IMPORTS #
###########
import os.path
import os
from multiprocessing import Manager
from multiprocessing.pool import Pool, ThreadPool, ApplyResult
import argparse
import datetime
import time
from threading import RLock
import logging
from abc import ABCMeta, abstractmethod
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s [%(module)s:%(funcName)s:%(lineno)d] %(message)s")

#############
# SINGLETON #
#############
class Singleton(type):
    """
    Classic singleton metaclass.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

###############
# FALSE POOLS #
###############
class SyncPool(dict):
    """
    A false pool that performs a synchronous computation instead of asynchronous.
    """

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
        result = ApplyResult(self, callback, error_callback)
        try:
            value = func(*args, **kwds)
            result._set(0, (True, value))
        except Exception as e:
            result._set(0, (False, e)) 
        finally:
            return result

class SilentPool(dict):
    """
    A false pool that does not perform the actual functions. 
    """

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
        result = ApplyResult(self, None, None)
        result._set(0, (True, 0))
        return result

##############
# DATA STORE #
##############
class AbstractDataStore(metaclass=ABCMeta):
    
    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def set_name(self, name):
        pass

    @abstractmethod
    def get_path(self):
        pass

    @abstractmethod
    def set_path(self, path):
        pass

    @abstractmethod
    def declare_entry(self, entry, on_push_callables, on_dump_callables, on_reset_callables):
        pass

    @abstractmethod
    def has_entry(self, entry):
        pass

    @abstractmethod
    def get_entries(self):
        pass

    @abstractmethod
    def is_empty(self):
        pass

    @abstractmethod
    def get_locker(self, entry):
        pass

    @abstractmethod
    def get_push_callables(self, entry):
        pass

    @abstractmethod
    def get_reset_callables(self, entry):
        pass

    @abstractmethod
    def get_dump_callables(self, entry):
        pass

    @abstractmethod
    def get_data(self, entry):
        pass

    @abstractmethod
    def append_data(self, entry, time, data):
        pass

    @abstractmethod
    def clear_data(self, entry):
        pass

    @abstractmethod
    def get_counter(self, entry):
        pass

class SynchronousDataStore(AbstractDataStore):

    def __init__(self):
        super(SynchronousDataStore).__init__()
        self._managed = argparse.Namespace()
        self._managed.name = "data-logger"
        self._managed.path = "."
        self._managed.entries = list()
        self._managed.data = dict()
        self._managed.lockers = dict()
        self._managed.counters = dict()
        self._managed.on_push_callables = dict()
        self._managed.on_reset_callables = dict()
        self._managed.on_dump_callables = dict()
    
    def get_name(self):
        return self._managed.name

    def set_name(self, name):
        self._managed.name = name
    
    def get_path(self):
        return self._managed.path

    def is_empty(self):
        return len(self._managed.entries) == 0 
    
    def set_path(self, path):
        self._managed.path = path

    def declare_entry(self, entry, on_push_callables, on_dump_callables, on_reset_callables):
        self._managed.entries.append(entry)
        self._managed.lockers[entry] = RLock()
        self._managed.data[entry] = dict()
        self._managed.counters[entry] = 0
        self._managed.on_push_callables[entry] = list(on_push_callables)
        self._managed.on_reset_callables[entry] = list(on_reset_callables)
        self._managed.on_dump_callables[entry] = list(on_dump_callables)

    def has_entry(self, entry):
        return entry in self._managed.entries      

    def get_entries(self):
        return self._managed.entries

    def get_locker(self, entry):
        return self._managed.lockers[entry]    

    def get_push_callables(self, entry):
        return self._managed.on_push_callables[entry]

    def get_reset_callables(self, entry):
        return self._managed.on_reset_callables[entry]

    def get_dump_callables(self, entry):
        return self._managed.on_dump_callables[entry]

    def get_data(self, entry):
        return self._managed.data[entry]

    def append_data(self, entry, time, data):
        self._managed.data[entry][time] = data 
        self._managed.counters[entry] += 1

    def clear_data(self, entry):
        self._managed.data[entry] = dict()
        self._managed.counters[entry] = 0

    def get_counter(self, entry):
        return self._managed.counters[entry]

class AsynchronousDataStore(AbstractDataStore):

    def __init__(self):
         super(AsynchronousDataStore).__init__()
         self._manager = Manager()
         self._managed = self._manager.Namespace()
         self._managed.name = "data-logger"
         self._managed.path = "."
         self._managed.entries = self._manager.list()
         self._managed.data = self._manager.dict()
         self._managed.lockers = self._manager.dict()
         self._managed.counters = self._manager.dict()
         self._managed.on_push_callables = self._manager.dict()
         self._managed.on_reset_callables = self._manager.dict()
         self._managed.on_dump_callables = self._manager.dict()
    
    def get_name(self):
        return self._managed.name

    def set_name(self, name):
        self._managed.name = name
    
    def get_path(self):
        return self._managed.path
    
    def is_empty(self):
        return len(self._managed.entries) == 0 

    def set_path(self, path):
        self._managed.path = path

    def declare_entry(self, entry, on_push_callables, on_dump_callables, on_reset_callables):
        self._managed.entries.append(entry)
        self._managed.lockers[entry] = self._manager.RLock()
        self._managed.data[entry] = self._manager.dict()
        self._managed.counters[entry] = 0
        self._managed.on_push_callables[entry] = self._manager.list(on_push_callables)
        self._managed.on_reset_callables[entry] = self._manager.list(on_reset_callables)
        self._managed.on_dump_callables[entry] = self._manager.list(on_dump_callables)

    def has_entry(self, entry):
        return entry in self._managed.entries      
  
    def get_entries(self):
        return self._managed.entries

    def get_locker(self, entry):
        return self._managed.lockers[entry]
        
    def get_push_callables(self, entry):
        return self._managed.on_push_callables[entry]

    def get_reset_callables(self, entry):
        return self._managed.on_reset_callables[entry]

    def get_dump_callables(self, entry):
        return self._managed.on_dump_callables[entry]

    def get_data(self, entry):
        return self._managed.data[entry]

    def append_data(self, entry, time, data):
        self._managed.data[entry][time] = data
        self._managed.counters[entry] += 1

    def clear_data(self, entry):
        self._managed.data[entry] = dict()
        self._managed.counters[entry] = 0

    def get_counter(self, entry):
        return self._managed.counters[entry]


###############
# DATA LOGGER #
###############
class DataLogger(metaclass=Singleton):
    """Stores and save various type of data under various forms."""

    @staticmethod
    def _error_callback(exception: Exception):
        """Called on exception"""
        print(f"Exception {repr(exception)} occurred during a handling.")

    @staticmethod
    def _push(store, entry, value, time):
        """Push method called by the pool executors"""
        with store.get_locker(entry):
            store.append_data(entry, time, value)
            for f in store.get_push_callables(entry):
                try:
                    f(entry, store.get_data(entry), path=store.get_path())
                except Exception as e:
                    logging.getLogger("datalogger").warning(f"{store.get_name()} DataLogger: function {f} of {entry} failed: {e}")

    @staticmethod
    def _dump(store, entry):
        """Dump method called by the pool executors"""
        with store.get_locker(entry):
            for f in store.get_dump_callables(entry):
                try:
                    f(entry, store.get_data(entry), path=store.get_path())
                except Exception as e:
                    logging.getLogger("datalogger").warning(f"{store.get_name()} DataLogger: function {f} of {entry} failed: {e}")

    @staticmethod
    def _reset(store, entry):
        """Inner reset method called by the pool executor"""
        with store.get_locker(entry):
            for f in store.get_reset_callables(entry):
                try:
                    f(entry, store.get_data(entry), path=store.get_path())
                except Exception as e:
                    logging.getLogger("datalogger").warning(f"{store.get_name()} DataLogger: function {f} of {entry} failed: {e}")
            store.clear_data(entry)

    def __init__(self):
        # Init and set attributes
        super(DataLogger, self).__init__()
        self._store = SynchronousDataStore()
        self._tick = datetime.datetime.now()
        self._results = list()
        self.set_pool("sync")
        # Log
        logging.getLogger("datalogger").info("{} DataLogger initialized!".format(self._store.get_name()))
 
    def set_path(self, path):
        """Sets the root path of the logger. Used by all the handlers that write on disk.

        :param string path: A valid path to write the data in.
        """
        if not self._store.is_empty():
            raise Exception("You tried to change logger path after having registered some entries.")
        os.makedirs(path, exist_ok=True)
        self._store.set_path(path)

    def get_path(self):
        """Returns the root path of the logger.

        :return: The root path of the logger.
        :rtype: string
        """
        return self._store.get_path()

    def set_pool(self, pool, n_par=5):
        """Sets the executor to be used to call handlers.
        The pool can be:
            + Thread based, in which case the `n_par` argument gives the number of threads executors.
            + Process based, in which case the `n_par` argument gives the number of process executors.
            + Synchronous, in which case whatever the `n_par`, the handlers are executed right away.
            + Silent, in which case, whatever the `n_par`, the handlers will not be executed. 

        :param string pool: The type of executor to use to call handlers. Either "thread", "process", "sync" or "silent".
        :param int n_par: The number of executor to use.
        """
        if not self._store.is_empty():
            raise Exception("You tried to pool after having registered some entries.")
        if pool == "thread":
            self._store = AsynchronousDataStore()
            self._pool = ThreadPool(n_par)
        elif pool == "process":
            self._store = AsynchronousDataStore()
            self._pool = Pool(n_par)
        elif pool == "sync":
            self._store = SynchronousDataStore()
            self._pool = SyncPool()
        elif pool == "silent":
            self._store = SynchronousDataStore()
            self._pool = SilentPool()
        else:
            raise Exception(f"Unknown pool type `{pool}`")

    def set_name(self, name):
        """Sets the name of the logger.

        :param string name: Name of the logger
        """
        self._store.set_name(name)

    def declare(self, entry, on_push_callables, on_dump_callables, on_reset_callables):
        """Register a recurring log entry.

        Registering an entry gives access to the `push`, `reset` and `dump` methods. Note that all the handlers must be
        able to handle the data that will be pushed.

        :param string entry: Name of the log entry.
        :param List[handlers] on_push_callables: Handlers called on data when `push` is called.
        :param List[handlers] on_reset_callables: Handlers called on data when `reset` is called.
        :param List[handlers] on_dump_callables: Handlers called on the data when `dump` is called.
        """
        if self._store.has_entry("entry"):
            raise Exception("You tried to declare an existing log entry")
        self._store.declare_entry(entry, on_push_callables, on_dump_callables, on_reset_callables)
        if os.path.dirname(entry) != "":
            os.makedirs(os.path.join(self._store.get_path(), os.path.dirname(entry)), exist_ok=True)

    def push(self, entry, value, time=None):
        """Append data to a recurring log.

        All handlers registered for the `on_push` event will be called.

        :param string entry: Name of the log entry
        :param Any value: Object containing the data to log. Should be of same type from call to call...
        :param int or None time: Date of the logging (epoch, iteration, tic ...). Will be used as key in the data
        dictionary. If `None`, the last data key plus one will be used.
        """
        result = self._pool.apply_async(DataLogger._push,
                                            (self._store,
                                             entry,
                                             value,
                                             time if time is not None else self._store.get_counter(entry)),
                                            error_callback=DataLogger._error_callback)
        self._results.append(result)

    def dump(self):
        """Calls handlers declared for `on_dump` event, for all registered log entries.
        """
        for entry in self._store.get_entries():
            result = self._pool.apply_async(DataLogger._dump,
                                                (self._store,
                                                 entry),
                                                error_callback=DataLogger._error_callback)
            self._results.append(result)

    def reset(self, entry):
        """Resets the data of a recurring log entry.

        All handlers registered for the `on_reset` event will be called before the storage is emptied.

        :param string entry: name of the log entry.
        """
        future = self._pool.apply_async(DataLogger._reset,
                                            (self._store,
                                             entry),
                                            error_callback=DataLogger._error_callback)
        self._results.append(future)

    def get_entry_length(self, entry):
        """Retrieves the number of data saved for a log entry.

        :param string entry: Name of the log entry
        :return: Number of data pieces in the entry storage
        :rtype: int
        """
        return self._store.get_counter(entry)

    def get_serie(self, entry):
        """Returns the data in a list ordered by keys.

        :param string entry: Name of the log entry
        :return: Serie of data ordered by key
        :rtype: List[any]
        """
        return [i[1] for i in sorted(self._store.get_data(entry).items())]

    def wait(self, log_durations=True):
        """Wait for the handling queue to be emptied.

        :param bool log_durations: Whether to log the wait duration.
        """
        # Using a Lock with timeout to wait allows to see it on concurrency diagrams.
        b = datetime.datetime.now()
        # We wait
        while True:
            self._results = list(filter(lambda r: not r.ready(), self._results))
            if self._results:
                time.sleep(.1)
            else:
                break
        if log_durations:
            logging.getLogger("datalogger").info(f"{self._store.get_name()} DataLogger: Last wait occured {b - self._tick} ago.")
            logging.getLogger("datalogger").info(f"{self._store.get_name()} DataLogger: Waited {datetime.datetime.now() - b} for completion.")
        self._tick = datetime.datetime.now()
