===================================
A Soccer Action Valuation Toolkit
===================================

`socceraction` is a Python package for objectively quantifying the value of
the individual actions performed by soccer players using event stream data. It
contains the following components:

- A set of API clients for **loading event stream data** from
  `StatsBomb <https://statsbomb.com/>`__, `Wyscout <https://wyscout.com/>`__
  and `Opta <https://www.statsperform.com/opta/>`__.
- Converters for each of these provider's proprietary data format to the
  **SPADL** and **atomic-SPADL** formats, which are unified and expressive
  languages for on-the-ball player actions.
- An implementation of the **Expected Threat (xT)** possession value framework.
- An implementation of the **VAEP** and **Atomic-VAEP** possession value frameworks.

.. image:: actions_bra-bel.png
  :width: 600
  :align: center


.. toctree::
   :hidden:
   :caption: Documentation

   documentation/intro
   documentation/install
   documentation/data/index
   documentation/spadl/index
   documentation/valuing_actions/index
   documentation/faq

.. toctree::
   :hidden:
   :caption: API reference

   api/data
   api/spadl_regular
   api/xthreat
   api/vaep
   api/spadl_atomic
   api/atomic_vaep

.. toctree::
   :hidden:
   :caption: Development

   Changelog <https://github.com/ML-KULeuven/socceraction/releases>
   development/developer_guide


First steps
===========

Are you new to soccer event stream data and possession value frameworks? Check
out our `interactive explainer <https://dtai.cs.kuleuven.be/sports/vaep>`__
and watch Lotte Bransen's and Jan Van Haaren's `presentation in Friends of
Tracking <https://www.youtube.com/watch?v=w0LX-2UgyXU>`__. Once familiar with
the basic concepts, you can move on to the :doc:`quickstart guide
<documentation/intro>` or continue with the hands-on video tutorials of the
Friends of Tracking series:

- Valuing actions in soccer (`video <https://www.youtube.com/watch?v=xyyZLs_N1F0>`__, `slides <https://drive.google.com/open?id=1t-jPgQFjZ7K4HRduaZWexUOMOmc1XR9H1jVWwaZYsOU>`__)
   This presentation expands on the content of the introductory presentation
   by discussing the technicalities behind the VAEP framework for valuing
   actions of soccer players as well as the content of the hands-on video
   tutorials in more depth.

- Tutorial 1: Run pipeline (`video <https://www.youtube.com/watch?v=0ol_eLLEQ64>`__, `notebook <https://github.com/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial1-run-pipeline.ipynb>`__, `notebook on Google Colab <https://colab.research.google.com/github/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial1-run-pipeline.ipynb>`__)
   This tutorial demonstrates the entire pipeline of ingesting the raw Wyscout
   match event data to producing ratings for soccer players. This tutorial
   touches upon the following four topics: downloading and preprocessing the
   data, valuing game states, valuing actions and rating players.

- Tutorial 2: Generate features (`video <https://www.youtube.com/watch?v=Ep9wXQgAFaE>`__, `notebook <https://github.com/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial2-generate-features.ipynb>`__, `notebook on Google Colab <https://colab.research.google.com/github/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial2-generate-features.ipynb>`__)
   This tutorial demonstrates the process of generating features and labels. This
   tutorial touches upon the following three topics: exploring the data in the
   SPADL representation, constructing features to represent actions and
   constructing features to represent game states.

- Tutorial 3: Learn models (`video <https://www.youtube.com/watch?v=WlORqYIb-Gg>`__, `notebook <https://github.com/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial3-learn-models.ipynb>`__, `notebook on Google Colab <https://colab.research.google.com/github/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial3-learn-models.ipynb>`__)
   This tutorial demonstrates the process of splitting the dataset into
   a training set and a test set, learning baseline models using conservative
   hyperparameters for the learning algorithm, optimizing the hyperparameters for
   the learning algorithm and learning the final models.

- Tutorial 4: Analyze models and results (`video <https://www.youtube.com/watch?v=w9G0z3eGCj8>`__, `notebook <https://github.com/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial4-analyze-models-and-results.ipynb>`__, `notebook on Google Colab <https://colab.research.google.com/github/SciSports-Labs/fot-valuing-actions/blob/master/notebooks/tutorial4-analyze-models-and-results.ipynb>`__)
   This tutorial demonstrates the process of analyzing the importance of
   the features that are included in the trained machine learning models,
   analyzing the predictions for specific game states, and analyzing the
   resulting player ratings.

.. note::

   The video tutorials are based on version 0.2.0 of the socceraction library.
   If a more recent version of the library is installed, the code may need to
   be adapted.


Getting help
============

Having trouble? We'd like to help!

* Try the :doc:`FAQ <documentation/faq>` -- it's got answers to many common questions.

* Looking for specific information? Try the :ref:`genindex` or :ref:`modindex`.

* Report bugs in our `ticket tracker`_.

.. _ticket tracker: https://github.com/ML-KULeuven/socceraction/issues


Contributing
============

Learn about the development process itself and about how you can contribute in our :doc:`developer guide <development/developer_guide>`.


Research
========

If you make use of this package in your research, please consider citing the
following papers.

- Tom Decroos, Lotte Bransen, Jan Van Haaren, and Jesse Davis. **"Actions speak
  louder than goals: Valuing player actions in soccer."** In Proceedings of the
  25th ACM SIGKDD International Conference on Knowledge Discovery & Data
  Mining, pp. 1851-1861. 2019.

  [`pdf`__, `bibtex`__]

- Maaike Van Roy, Pieter Robberechts, Tom Decroos, and Jesse Davis. **"Valuing on-the-ball actions in soccer:
  a critical comparison of xT and VAEP."** In Proceedings of the AAAI-20
  Workshop on Artifical Intelligence in Team Sports. AI in Team Sports
  Organising Committee, 2020.

  [`pdf`__, `bibtex`__]

__ http://doi.acm.org/10.1145/3292500.3330758
__ https://raw.githubusercontent.com/ML-KULeuven/socceraction/master/docs/_static/decroos19.bibtex
__ https://limo.libis.be/primo-explore/fulldisplay?docid=LIRIAS2913207&context=L&vid=KULeuven&search_scope=ALL_CONTENT&tab=all_content_tab&lang=en_US
__ https://raw.githubusercontent.com/ML-KULeuven/socceraction/master/docs/_static/vanroy20.bibtex
