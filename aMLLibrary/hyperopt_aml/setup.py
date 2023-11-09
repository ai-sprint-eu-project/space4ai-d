import io
import re

import setuptools


with open("hyperopt/__init__.py", encoding="utf8") as f:
    version = re.search(r"__version__ = \"(.*?)\"", f.read()).group(1)
    if version is None:
        raise ImportError("Could not find __version__ in hyperopt/__init__.py")

setuptools.setup(
    name="hyperopt",
    version=version,
    packages=setuptools.find_packages(include=["hyperopt*"]),
    url="https://hyperopt.github.io/hyperopt",
    author="James Bergstra",
    author_email="james.bergstra@gmail.com",
    description="Distributed Asynchronous Hyperparameter Optimization",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Environment :: Console",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
    ],
    platforms=["Linux", "OS-X", "Windows"],
    license="BSD",
    keywords="Bayesian optimization hyperparameter model selection",
    include_package_data=True,
    install_requires=[
        "numpy>=1.17",
        "scipy",
        "six",
        "networkx>=2.2",
        "future",
        "tqdm",
        "cloudpickle",
    ],
    extras_require={
        "dev": ["black", "pre-commit", "pytest"],
    },
    tests_require=["pytest"],
    zip_safe=False,
)
