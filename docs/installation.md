# Installation

## Conda

The foolproof way to install ducknx is with [conda](https://conda.io/) or [mamba](https://mamba.readthedocs.io/):

```shell
conda create -n dx conda-forge::ducknx
```

This creates a new conda environment and installs ducknx into it, via the conda-forge channel. If you want other packages, such as `jupyterlab`, installed in this environment as well, just add their names after `ducknx` above. To upgrade ducknx to a newer release, remove the conda environment you created and then create a new one again, as above. See the [conda](https://conda.io/) and [conda-forge](https://conda-forge.org/) documentation for more details.

## Docker

You can run ducknx + JupyterLab directly from the official ducknx [Docker](https://hub.docker.com/r/gboeing/osmnx) image.

## Pip

You can also install ducknx with [uv](https://docs.astral.sh/uv/) or [pip](https://pip.pypa.io/) (into a virtual environment):

```shell
pip install ducknx
```

ducknx is written in pure Python and distributed on [PyPI](https://pypi.org/project/osmnx/). Its installation alone is thus trivially simple if you have its dependencies installed and tested on your system. However, ducknx depends on other packages that in turn depend on compiled C/C++ libraries, which may present some challenges depending on your specific system's configuration. If precompiled binaries are not available for your system, you may need to compile and configure those dependencies by following their installation instructions. So, if you're not sure what you're doing, just follow the conda instructions above to avoid installation problems.
