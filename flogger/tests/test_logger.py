import flogger as fl
import numpy as np

if __name__ == "__main__":

    a = fl.DataLogger()
    a.set_path("./test_output")
    a.set_pool("sync", 10)
    a.set_name("kikou")
    a.declare("Loss",
              [fl.echo_last, fl.add_tsb_scalar_last, fl.save_to_json],
              [],
              [fl.save_to_json, fl.save_to_mpl_lines])
    a.declare("Weights",
              [],
              [],
              [fl.save_to_mpl_histolines])
    a.push("Loss", 50, 1)
    a.push("Weights", np.histogram_bin_edges(np.random.randn(10000), 7), 1)
    a.push("Loss", 50, 2)
    a.push("Weights", np.histogram_bin_edges(np.random.randn(10000) + 10, 7), 2)
    a.push("Loss", 100, 5)
    a.push("Weights", np.histogram_bin_edges(np.random.randn(10000) + 15, 7), 5)
    a.declare("Gif",
              [fl.save_to_gif_last],
              [],
              [])
    a.push("Gif", np.random.randint(0, 255, (50, 1, 40, 40)).astype(np.uint8), 1)
    a.wait()
    for i in range(100):
        a.push("Loss", np.random.randn(), i)
    a.wait()
    a.dump()
