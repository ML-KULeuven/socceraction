===================
Installation
===================

Before you can use socceraction, you'll need to get it installed. This guide
will guide you to a minimal installation that'll work while you walk through
the introduction.

Install Python
==============

Being a Python library, socceraction requires Python. 
Currently, socceraction supports Python version 3.5 -- 3.8.
Get the latest version of Python at https://www.python.org/downloads/ or with
your operating system's package manager.

You can verify that Python is installed by typing ``python`` from your shell;
you should see something like::

		Python 3.x.y
		[GCC 4.x] on linux
		Type "help", "copyright", "credits" or "license" for more information.
		>>>

Install socceraction
====================

You've got two options to install socceraction.

.. _installing-official-release:

Installing an official release with ``pip``
-------------------------------------------

This is the recommended way to install socceraction. Simply run this simple command in your terminal of choice:

.. code-block:: console

		 $ python -m pip install socceraction


You might have to install pip first. The easiest method is to use the `standalone pip installer`_.  

.. _pip: https://pip.pypa.io/
.. _standalone pip installer: https://pip.pypa.io/en/latest/installing/#installing-with-get-pip-py


.. _installing-development-version:

Installing the development version
----------------------------------

Socceraction is actively developed on GitHub, where the code is
`always available <https://github.com/ML-KULeuven/socceraction>`_.

You can either clone the public repository:

.. code-block:: console

		$ git clone git://github.com/ML-KULeuven/socceraction.git

Or, download the `zipball <https://github.com/ML-KULeuven/socceraction/archive/master.zip>`_:

.. code-block:: console

		$ curl -OL https://github.com/ML-KULeuven/socceraction/archive/master.zip

Once you have a copy of the source, you can embed it in your own Python
package, or install it into your site-packages easily:

.. code-block:: console

	$ cd socceraction
	$ python -m pip install .

Verifying
=========

To verify that socceraction can be seen by Python, type ``python`` from your shell.
Then at the Python prompt, try to import socceraction:

.. parsed-literal::

    >>> import socceraction
