import setuptools  # type: ignore

long_description = """
Socceraction is a Python package containing

- **SPADL** (Soccer Player Action Description Language): a unified and expressive language for on-the-ball player actions in soccer
- **VAEP** (Valuing Actions by Estimating Probabilities): a framework to value actions on their expected impact on the score line
- **xT** (Expected Threat): an alternative framework to value ball-progressing actions using a possession-based Markov model.

For more information, see https://github.com/ML-KULeuven/socceraction
"""

setuptools.setup(
    name='socceraction',
    version='1.0.2',
    description='Convert soccer event stream data to the SPADL format and value on-the-ball player actions',
    url='https://github.com/ML-KULeuven/socceraction',
    author='Tom Decroos',
    author_email='tom.decroos.be@gmail.com',
    license='MIT',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "cython",  # required for scikit-learn
        "lxml",
        "pandas>=1.1.0",
        "packaging>=20.0",  # required until https://github.com/pandera-dev/pandera/pull/380 is released
        "pandera>=0.6.1",
        "requests",
        "scikit-learn",
        "tqdm",
        "unidecode"
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
