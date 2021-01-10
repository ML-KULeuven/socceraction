Developer guide
================

This document lays out guidelines and advice for contributing to this project.
If you're thinking of contributing, please start by reading this document and
getting a feel for how contributing to this project works. If you have any
questions, feel free to reach out to either `Tom Decroos`_, or `Pieter
Robberechts`_, the primary maintainers.

.. _Tom Decroos: https://tomdecroos.github.io
.. _Pieter Robberechts: https://people.cs.kuleuven.be/~pieter.robberechts/

The guide is split into sections based on the type of contribution you're
thinking of making.

Code Contributions
------------------

If you intend to contribute code, do not feel the need to sit on your
contribution until it is perfectly polished and complete. It helps everyone
involved for you to seek feedback as early as you possibly can. Submitting an
early, unfinished version of your contribution for feedback can save you from
putting a lot of work into a contribution that is not suitable for the
project.


Steps for Submitting Code
~~~~~~~~~~~~~~~~~~~~~~~~~~

When contributing code, you'll want to follow this checklist:

1. Fork the repository on GitHub.
2. Run the tests to confirm they all pass on your system. If they don't, you'll
   need to investigate why they fail. If you're unable to diagnose this
   yourself, raise it as a bug report.
3. Write tests that demonstrate your bug or feature. Ensure that they fail.
4. Make your change.
5. Run the entire test suite again, confirming that all tests pass *including
   the ones you just added*.
6. Make sure your code follows the code style discussed below.
7. Send a GitHub Pull Request to the main repository's ``master`` branch.
   GitHub Pull Requests are the expected method of code collaboration on this
   project.

Code Style
~~~~~~~~~~~~

The socceraction codebase uses the `PEP 8`_ code style. In addition, we have
a few guidelines:

- Line-length can exceed 79 characters, to 100, when convenient.
- Line-length can exceed 100 characters, when doing otherwise would be *terribly* inconvenient.
- Always use single-quoted strings (e.g. ``'#soccer'``), unless a single-quote occurs within the string.

To ensure all code conforms to this format. You can format the code using
`black`_ prior to committing.

Docstrings are to follow the `numpydoc guidelines`_.

.. _PEP 8: https://pep8.org/
.. _black: https://black.readthedocs.io/en/stable/
.. _numpydoc guidelines: https://numpydoc.readthedocs.io/en/latest/format.html

Documentation Contributions
---------------------------

Documentation improvements are always welcome! The documentation files live in
the ``docs/`` directory of the codebase. They're written in
`reStructuredText`_, and use `Sphinx`_ to generate the full suite of
documentation.

When contributing documentation, please do your best to follow the style of the
documentation files. This means a soft-limit of 79 characters wide in your text
files and a semi-formal, yet friendly and approachable, prose style.

When presenting Python code, use single-quoted strings (``'hello'`` instead of
``"hello"``).

.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/index.html


.. _bug-reports:

Bug Reports
-----------

Bug reports are hugely important! Before you raise one, though, please check
through the `GitHub issues`_, **both open and closed**, to confirm that the bug
hasn't been reported before. Duplicate bug reports are a huge drain on the time
of other contributors, and should be avoided as much as possible.

.. _GitHub issues: https://github.com/ML-KULeuven/socceraction/issues


Feature Requests
----------------

Socceraction is not actively developed. It's primary use is to enable
reproducability of our research. If you believe there is a feature missing,
feel free to raise a feature request, but please do be aware that the
overwhelming likelihood is that your feature request will not be accepted.
