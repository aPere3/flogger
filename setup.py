from setuptools import setup

setup(name='flogger',
      version='0.1.7',
      description='Data logging made harder',
      url='http://github.com/aPere3/flogger',
      author='Alexandre Péré',
      author_email='alexandre.pere@inria.fr',
      license='MIT',
      packages=['flogger'],
      install_requires=[
            "matplotlib",
            "numpy",
            "imageio",
            "tensorboardX",
      ],
      zip_safe=False)
