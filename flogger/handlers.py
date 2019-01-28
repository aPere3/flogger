#!/usr/bin/env python
# coding: utf-8
"""
This module contains handlers for the DataLogger class.
"""
###########
# IMPORTS #
###########
import sys
import os
import logging
import tensorboardX
import imageio
import numpy as np
import json
from pprint import pformat
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
tsb_writer = tensorboardX.SummaryWriter(log_dir="/tmp")

############
# HANDLERS #
############
def echo_last(entry, data, output="stdout", **kwargs):
    """Handler that prints the last data item to a standard stream.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    :param string output: The stream to echo to. Either "stdout" or "stderr".
    """
    last_time = max(data.keys())
    value = data[last_time]
    if output == "stdout":
        print("{} at {}: {}".format(entry, last_time, value))
    elif output == "stderr":
        print("{} at {}: {}".format(entry, last_time, value), file=sys.stderr)


def log_debug_last(entry, data, **kwargs):
    """Handler that logs a debug level message of the last data item.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    """
    last_time = max(data.keys())
    value = data[last_time]
    logging.getLogger("datalogger").debug("{} at {}: {}".format(entry, last_time, value))


def log_info_last(entry, data, **kwargs):
    """Handler that logs an info level message of the last data item.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    """
    last_time = max(data.keys())
    value = data[last_time]
    logging.getLogger("datalogger").info("{} at {}: {}".format(entry, last_time, value))


def log_warning_last(entry, data, **kwargs):
    """Handler that logs a warning level message of the last data item.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    """
    last_time = max(data.keys())
    value = data[last_time]
    logging.getLogger("datalogger").warning("{} at {}: {}".format(entry, last_time, value))


def log_error_last(entry, data, **kwargs):
    """Handler that logs an error level message of the last data item.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    """
    last_time = max(data.keys())
    value = data[last_time]
    logging.getLogger("datalogger").error("{} at {}: {}".format(entry, last_time, value))


def log_critical_last(entry, data, **kwargs):
    """Handler that logs a critical level message of the last data item.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable.
    """
    last_time = max(data.keys())
    value = data[last_time]
    logging.getLogger("datalogger").critical("{} at {}: {}".format(entry, last_time, value))


def add_tsb_scalar_last(entry, data, subfolder="", path=".", **kwargs):
    """Handler that appends the last item of the data dictionary to a tensorboard event file.

    :param string entry: Name of the log entry
    :param Dict data: Data should be a number
    :param string subfolder: Subfolder in which put the data
    :param string path: Root path. Set by DataLogger if used as handler.
    """

    tsb_dir = os.path.join(path, subfolder)
    os.makedirs(tsb_dir, exist_ok=True)
    if tsb_writer.file_writer.get_logdir() != tsb_dir:
        tsb_writer.file_writer = tensorboardX.FileWriter(tsb_dir)
    last_time = max(data.keys())
    value = data[last_time]
    tsb_writer.add_scalar(entry, value, last_time)


def add_tsb_scalars_last(entry, data, labels=None, path=".", subfolder="", **kwargs):
    """Handler that appends the last item of the data dictionary to a tensorboard event file.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be numpy arrays of size [n] with constant n.
    :param List[string] labels: Labels to use for the lines
    :param string subfolder: Subfolder in which put the data
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    tsb_dir = os.path.join(path, subfolder)
    os.makedirs(tsb_dir, exist_ok=True)
    if tsb_writer.file_writer.get_logdir() != tsb_dir:
        tsb_writer.file_writer = tensorboardX.FileWriter(tsb_dir)
    last_time = max(data.keys())
    value = data[last_time]
    if labels is None:
        labels = [str(a) for a in range(len(value))]
    scalars_dict = {labels[i]: value[i] for i in range(len(value))}
    tsb_writer.add_scalars(entry, scalars_dict, last_time)


def add_tsb_image_last(entry, data, path=".", subfolder="", **kwargs):
    """Handler that append the last image of the data to a tensorboard event file.

    :param string entry: Name of the log entry
    :param Dict data: Data should be a numpy array / torch tensor of shape [3, W, H]
    :param string subfolder: Subfolder in which put the data
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    tsb_dir = os.path.join(path, subfolder)
    os.makedirs(tsb_dir, exist_ok=True)
    if tsb_writer.file_writer.get_logdir() != tsb_dir:
        tsb_writer.file_writer = tensorboardX.FileWriter(tsb_dir)
    last_time = max(data.keys())
    value = data[last_time]
    tsb_writer.add_image(entry, value, last_time)


def save_to_gif(entry, data, path=".", **kwargs):
    """Handler that stores all images of the data dictionary to a gif.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [W, H]
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    values = [data[i] for i in sorted(data.keys())]
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = imageio.get_writer("{}.gif".format(path), fps=5)
    for frame in values:
        writer.append_data(frame)
    writer.close()


def save_to_gif_last(entry, data, fps=5, path=".", **kwargs):
    """Handler that stores the last item of the data dictionary to a gif.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [N, 3, W, H] or of shape [N, 1, W, H]
    :param int fps: The framerate
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    last_time = max(data.keys())
    value = data[last_time]
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = imageio.get_writer("{}.gif".format(path), fps=fps)
    for frame in value:
        writer.append_data(np.moveaxis(frame, 0, -1))
    writer.close()


def save_to_jpg(entry, data, path=".", **kwargs):
    """Handler that stores all images of a dictionary to a concatenated jpg.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [3, W, H]
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    values = list(data.values())
    image = np.concatenate(values, axis=1)
    image = np.moveaxis(image, 0, -1)
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    imageio.imwrite("{}.jpg".format(path), image)


def save_to_jpg_last(entry, data, path=".", **kwargs):
    """Handler that stores the last item of the data dictionary to a jpg.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [3, W, H] or of shape [1, W, H]
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    imageio.imwrite("{}.jpg".format(os.path.join(path, entry)), list(data.values())[-1])


def save_to_mp4(entry, data, fps=5, path=".", **kwargs):
    """Handler that stores all images of the data dictionary to a mp4.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [3, W, H]
    :param int fps: Framerate
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    values = [data[i] for i in sorted(data.keys())]
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = imageio.get_writer("{}.mp4".format(path), fps=fps)
    for frame in values:
        writer.append_data(np.moveaxis(frame, 0, -1))
    writer.close()


def save_to_mp4_last(entry, data, fps=5, path=".", **kwargs):
    """Handler that stores the last item of the data dictionary to a mp4.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [N, 3, W, H] or of shape [N, 1, W, H]
    :param int fps: Framerate
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    last_time = max(data.keys())
    value = data[last_time]
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = imageio.get_writer("{}.mp4".format(path), fps=fps)
    for frame in value:
        writer.append_data(np.moveaxis(frame, 0, -1))
    writer.close()


def save_to_json(entry, data, path=".", **kwargs):
    """Handler that stores the whole data dictionary in a json file named after the log entry.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be JSON serializable data.
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open('{}.json'.format(path), 'w') as fp:
        json.dump(dict(data), fp)


def save_to_json_last(entry, data, path=".", **kwargs):
    """Handler that stores the last item in data dictionary in a json file named after the log entry.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be JSON serializable data.
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    last_time = max(data.keys())
    value = data[last_time]
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open('{}.json'.format(path), 'w') as fp:
        json.dump(value, fp)


def save_to_text_last(entry, data, path=".", **kwargs):
    """Handler that stores the last item in data dictionary in a text file named after the log entry.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    last_time = max(data.keys())
    value = pformat(data[last_time])
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open('{}.txt'.format(path), 'w') as fp:
        fp.write(value)


def save_to_text(entry, data, path=".", **kwargs):
    """Handler that stores the whole data dictionary in a text file named after the log entry.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be printable
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    value = pformat(data)
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open('{}.txt'.format(path), 'w') as fp:
        fp.write(value)


def save_to_mpl_lines(entry, data, labels=None, path=".", **kwargs):
    """Handler that stores the whole data dictionary in a matplotlib line figure written in a file named after the
    log entry.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be a numpy array of shape [N]
    :param List[string] labels: Labels to use for the plot
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    fig = plt.figure()
    plt.plot(np.array(list(data.keys())), np.array(list(data.values())))
    if labels:
        plt.legend(labels)
    plt.title(entry)
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    plt.savefig("{}.png".format(path))
    plt.close(fig)


def save_to_mpl_histolines(entry, data, color="navy", path=".", **kwargs):
    """Handler that stores the whole data dictionary in a matplotlib histogram lines figure.

    The data dictionary must store numpy vectors representing the bounds of the different histogram
    classes. Those can be extracted calling `np.histogram_bin_edges( . , 10)` with a odd number of classes.

    :param string entry: Name of the log entry.
    :param Dict data: Data should be numpy arrays of shape [N]
    :param string color: Color of the plot. Must be a valid matplotlib color string.
    :param string path: Root path. Set by DataLogger if used as handler.
    """
    fig = plt.figure()
    histograms = np.array(list(data.values()))
    for i in range(histograms[0].size // 2):
        plt.fill_between(np.array(list(data.keys()), dtype=np.uint16), histograms[:, i], histograms[:, -i - 1],
                         alpha=0.05, color=color)
    plt.title(entry)
    path = os.path.join(path, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    plt.savefig("{}.png".format(path))
    plt.close(fig)
