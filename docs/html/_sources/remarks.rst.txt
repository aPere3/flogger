Some important remarks
**********************

Here are the tricky pieces:

Error handling
^^^^^^^^^^^^^^
Note that when using ``push``, ``reset`` and ``dump``, the exceptions that may occur in handlers are just
reported, but are not raised. This allows to avoid stopping the code for errors in the logging...

Singleton
^^^^^^^^^
The logger is a singleton. This means that you can plug it in various places in your code, being sure that
only one instance is effectively used. From our experience, it is better practice to put small pieces of logging
close to the important pieces of the code, rather than logging everything at a single location. For instance, if you
have multiple classes with different levels of hierarchy in your algorithm, don't hesitate to put logs in every
classes to log various types of information. But to avoid any problems, try to set the logger ``path`` only once.

Asynchronous handling
^^^^^^^^^^^^^^^^^^^^^
The handling of data can both be made synchronously or asynchronously, by using `.set_pool`. In the synchronous case,
the handlers are sequentially executed in the experiment thread when ``push``, ``reset``, or ``dump`` are called. In the
asynchronous case, handlers are not called by your experiment thread, but are defered to separate threads or processes 
(depending on your preferences). This allows to temper the impact of logging on the performances of your experiment. 
For this to work, your data have to be **pickable**. 

Depending on the quantity of data and the quantity of handler calls, you may prefer to use synchronous or asynchronous
handling. Basically, if your logs are very quick to execute and use few data, you are better off using synchronous handling.
In the contrary, if you have large pieces of data such as images, or your handlers contain heavy computation, you should
be using asynchronous handling to avoid stopping the experiment thread for too long.

This being said, as you may know, python parallel capabilities are not quite perfect, and depending on how heavy are
your logging and your data, using threads or processes for the asynchronous handling may give different improvements. 
Also, increasing the number of threads or processes, may not end up in an increase of the data handling rate. As of 
now, we don't have particular advices to tune that, other than testing and seeing how the handling rate changes.

To do that, you can use the ``wait()`` method. This method will wait for the handling queue to be emptied, and will log
the duration of data handling in your console. It may be a good idea to call this after each iteration of your
algorithms, to see how it changes with different logging parameters.

Partial handlers
^^^^^^^^^^^^^^^^
Some handlers allows for extra keyword arguments (for example the color of a plot, or its title ...). You can set those
when registring by using partial functions::
   from functools import partial

   dl.declare("entry", [],
                       [],
                       [partial(fl.save_to_mpl_histolines, color="red")])


Logs folder structure
^^^^^^^^^^^^^^^^^^^^^
The ``DataLogger`` has a ``path`` attribute that can be set through the ``set_path`` method. The path cannot be set
after some recurring entries has been registered. This path is used as root path by all the handlers that write on disk.

The structure of this folder can be adjusted by naming the entries with forward slashes. For example, if the ``VAE/Loss``
entry was registered with a ``save_to_mpl_lines`` handlers, the figure would be created in a ``VAE`` subfolder, and
would be named ``Loss.png``.
