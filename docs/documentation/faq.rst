===
FAQ
===

**Q: What is socceraction?**
Socceraction is an an open-source Python package that primarily provides an
implementation of the VAEP possession value framework. However, the package also
provides a number of other features, such as API clients for loading data from
the most popular data providers and converters for each of these data
provider's proprietary data formats to a common action-based data format
(i.e., SPADL) that enables subsequent data analysis. Therefore, socceraction
can take away some of the heavy data preprocessing burden from researchers and
data scientists who are interested in working with soccer event stream data.

**Q: Where can I get event stream data?**
Both StatsBomb and Wyscout provide a free sample of their data. Alternatively
you can buy a subscription to the event data feed from StatsBomb, Wyscout or
Opta (Stats Perform). Instructions on how to load the data from each of these
sources with socceraction are avaliable in the :doc:`documentation
<data/index>`.

**Q: What license is socceraction released under?** Socceraction is released
under the `MIT license <https://github.com/ML-KULeuven/socceraction/blob/master/LICENSE.rst>`_.
You are free to use, modify and redistribute socceraction in any way you see
fit. However, if you do use socceraction in your research, please cite our
`research papers <Research>`_. When you use socceraction in public work
or when building a product or service using socceraction, we kindly request
that you include the following attribution text in all advertising and documentation::

  This product includes socceraction created by the <a href="https://dtai.cs.kuleuven.be/sports/">DTAI Sports Analytics lab</a>,
  available from <a href="https://github.com/ML-KULeuven/socceraction">https://github.com/ML-KULeuven/socceraction</a>.
