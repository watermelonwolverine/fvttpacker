# execute `source scripts/init_pythonpath.sh` before executing this

import os
from glob import glob

import setuptools

from fvttpacker import __version__
from fvttpacker.__constants import app_name, author, url, issues_url

setuptools.setup(
    name=app_name,
    version=__version__,
    author=author,
    author_email="29666253+watermelonwolverine@users.noreply.github.com",
    description="Packs and unpacks FoundryVTT databases",
    url=url,
    project_urls={
        "Bug Tracker": issues_url,
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
    py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob('src/*.py')],
    entry_points={
        'console_scripts': [f'{app_name} = fvttpacker.__cli_wrapper.main:cli'],
    },
    install_requires=["appdirs", "click", "plyvel"]
)
