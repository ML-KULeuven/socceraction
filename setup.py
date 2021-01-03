import setuptools # type: ignore

long_description = """
Socceraction is a Python package containing
- **SPADL** (Soccer Player Action Description Language): a unified and expressive language for on-the-ball player actions in soccer
- **VAEP** (Valuing Actions by Estimating Probabilities): a framework to value actions on their expected impact on the score line
- **xT** (Expected Threat): an alternative framework to value ball-progressing actions using a possession-based Markov model.
For more information, see https://github.com/ML-KULeuven/socceraction
"""

setuptools.setup(name='socceraction',
      version='0.2.1',
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
     install_requires=[
       "tqdm",
       "pandas",
       "numpy",
       "unidecode",
       "requests",
       "lxml"
     ],
     long_description=long_description,
     long_description_content_type='text/markdown',
      )
