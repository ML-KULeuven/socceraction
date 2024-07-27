"""Evaluation metrics for socceraction models."""

import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import numpy.ma as ma
from matplotlib import gridspec
from matplotlib.ticker import MaxNLocator, MultipleLocator
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
from scipy import integrate
from sklearn.metrics import auc, roc_curve
from sklearn.neighbors import KernelDensity
from sklearn.preprocessing import label_binarize
from sklearn.utils import check_consistent_length, column_or_1d


def plot_reliability_diagram(  # noqa: C901
    labels,
    scores,
    legend=None,
    show_histogram=True,
    bins=10,
    bin_strategy="uniform",
    bayesian=False,
    min_samples=None,
    fig=None,
    show_counts=False,
    ci=None,
    shaded_ci=False,
    interval_method="beta",
    fmt="s-",
    show_correction=False,
    show_gaps=False,
    sample_proportion=0,
    color_list=None,
    show_bars=False,
    invert_histogram=False,
    overlay_histogram=False,
    color_gaps="lightcoral",
    ax=None,
):
    """Plot the reliability diagram of the given scores and true labels.

    Parameters
    ----------
    labels : array (n_samples, )
        Labels indicating the true class.
    scores : array (n_samples,) or list of matrices
        Output probability scores for one or several methods.
    legend : list of strings or None
        Text to use for the legend.
    show_histogram : boolean
        If True, it generates an additional figure showing the number of
        samples in each bin.
    bins : int or list of floats
        Number of bins to create in the scores' space, or list of bin
        boundaries.
    bin_strategy : {'uniform', 'quantile'}, default='uniform'
        Strategy used to define the widths of the bins.

        uniform
            The bins have identical widths.
        quantile
            The bins have the same number of samples and depend on `y_prob`.
    bayesian : bool, default=False
        Compute true and predicted probabilities for a calibration curve using
        kernel density estimation instead of bins with a fixed width.
    min_samples : int or None
        Hide bins with less than 'min_samples'.
    fig : matplotlib.pyplot.Figure or None
        Figure to use for the plots, if None a new figure is created.
    show_counts : boolean
        If True shows the number of samples of each bin in its corresponding
        line marker.
    ci : float or None
        If a float between 0 and 1 is passed, it shows an errorbar
        corresponding to a confidence interval containing the specified
        percentile of the data.
    shaded_ci : boolean
        If True, the confidence interval is shown as a shaded area instead of
        error bars.
    interval_method : string (default: 'beta')
        Method to estimate the confidence interval which uses the function
        proportion_confint from statsmodels.stats.proportion
    fmt : string (default: 's-')
        Format of the lines following the matplotlib.pyplot.plot standard.
    show_correction : boolean
        If True shows an arrow for each bin indicating the necessary correction
        to the average scores in order to be perfectly calibrated.
    show_gaps : boolean
        If True shows the gap between the average predictions and the true
        proportion of positive samples.
    sample_proportion : float in the interval [0, 1] (default 0)
        If bigger than 0, it shows the labels of the specified proportion of
        samples.
    color_list : list of strings or None
        List of string colors indicating the color of each method.
    show_bars : boolean
        If True shows bars instead of lines.
    invert_histogram : boolean
        If True shows the histogram with the zero on top and highest number of
        bin samples at the bottom.
    overlay_histogram : boolean
        If True, shows the histogram on the same plot as the reliability diagram.
    color_gaps : string
        Color of the gaps (if shown).
    ax : matplotlib.pyplot.Axes or None
        Axes to use for the plots, if None a new axes is created.

    Returns
    -------
    fig : matplotlib.pyplot.figure
        Figure with the reliability diagram

    Raises
    ------
    ValueError
        If the number of classes is not 2 (non-binary classification).
    """
    if isinstance(scores, list):
        scores_list = scores
    else:
        scores_list = [
            scores,
        ]
    n_scores = len(scores_list)
    if color_list is None:
        color_list = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    classes = np.unique(labels)
    n_classes = len(classes)
    if n_classes != 2:
        raise ValueError("Only binary classification is supported. Provided labels %s." % labels)
    labels = label_binarize(labels, classes=classes)[:, 0]

    labels_list = []

    if fig is None:
        fig = plt.figure(figsize=(4, 4))

    if show_histogram:
        spec = gridspec.GridSpec(
            ncols=1, nrows=2, height_ratios=[5, 1], wspace=0.02, hspace=0.04, left=0.15
        )
    else:
        spec = gridspec.GridSpec(ncols=1, nrows=1, hspace=0.04, left=0.15)

    if isinstance(bins, int):
        n_bins = bins
        if bin_strategy == "quantile":  # Determine bin edges by distribution of data
            quantiles = np.linspace(0, 1, n_bins + 1)
            bins = np.percentile(scores_list[0], quantiles * 100)
            bins[0] = 0 - 1e-8
            bins[-1] = 1 + 1e-8
        elif bin_strategy == "uniform":
            bins = np.linspace(0, 1 + 1e-8, n_bins + 1)
        else:
            raise ValueError(
                "Invalid entry to 'strategy' input. Strategy "
                "must be either 'quantile' or 'uniform'."
            )
    elif isinstance(bins, list) or isinstance(bins, np.ndarray):
        n_bins = len(bins) - 1
        bins = np.array(bins)
        if bins[0] == 0.0:
            bins[0] = 0 - 1e-8
        if bins[-1] == 1.0:
            bins[-1] = 1 + 1e-8
    else:
        raise ValueError(
            "Invalid entry to 'bins' input. The must be either "
            "a list of bin boundaries or the number of bins."
        )

    if ax is not None:
        ax1 = ax
    else:
        ax1 = fig.add_subplot(spec[0])
    # Perfect calibration
    ax1.plot([0, 1], [0, 1], "--", color="lightgrey", zorder=10)
    for j, score in enumerate(scores_list):
        if labels_list:
            labels = labels_list[j]

        if bayesian:
            avg_true, avg_pred, bin_true, bin_total = bayesian_calibration_curve(labels, score)
            bins = np.linspace(0.01, 0.99, 99)
        else:
            avg_true, avg_pred, bin_true, bin_total = calibration_curve(labels, score, bins=bins)

        zero_idx = bin_total == 0

        if min_samples is not None:
            avg_true = ma.array(avg_true)
            avg_true[bin_total < min_samples] = ma.masked
            avg_pred = ma.array(avg_pred)
            avg_pred[bin_total < min_samples] = ma.masked

        name = legend[j] if legend else None
        if show_bars:
            ax1.bar(
                x=bins[:-1][~zero_idx],
                height=avg_true[~zero_idx],
                align="edge",
                width=(bins[1:] - bins[:-1])[~zero_idx],
                edgecolor="black",
                color=color_list[j],
            )
        else:
            if ci is None:
                ax1.plot(avg_pred, avg_true, fmt, label=name, color=color_list[j])
            else:
                from statsmodels.stats.proportion import proportion_confint

                nozero_intervals = proportion_confint(
                    count=bin_true[~zero_idx],
                    nobs=bin_total[~zero_idx],
                    alpha=1 - ci,
                    method=interval_method,
                )
                nozero_intervals = np.array(nozero_intervals)

                intervals = np.empty((2, bin_total.shape[0]))
                intervals.fill(np.nan)
                intervals[:, ~zero_idx] = nozero_intervals

                yerr = np.abs(intervals - avg_true)
                if shaded_ci:
                    ax1.fill_between(
                        avg_pred,
                        avg_true - yerr[0],
                        avg_true + yerr[1],
                        color=color_list[j],
                        alpha=0.2,
                    )
                    ax1.plot(avg_pred, avg_true, fmt, label=name, color=color_list[j])
                else:
                    ax1.errorbar(
                        avg_pred, avg_true, yerr=yerr, label=name, fmt=fmt, color=color_list[j]
                    )  # markersize=5)

        if show_counts:
            for ap, at, count in zip(avg_pred, avg_true, bin_total):
                if np.isfinite(ap) and np.isfinite(at):
                    ax1.text(
                        ap,
                        at,
                        str(count),
                        fontsize=6,
                        ha="center",
                        va="center",
                        zorder=11,
                        bbox={"boxstyle": "square,pad=0.3", "fc": "white", "ec": color_list[j]},
                    )

        if show_correction:
            for ap, at in zip(avg_pred, avg_true):
                ax1.arrow(
                    ap,
                    at,
                    at - ap,
                    0,
                    color=color_gaps,
                    head_width=0.02,
                    length_includes_head=True,
                    width=0.01,
                )

        if show_gaps:
            error = avg_pred - avg_true
            negative_values = error < 0
            ygaps = np.zeros(shape=(2, avg_true.shape[0]))
            ygaps[0, negative_values] = -error[negative_values]
            ygaps[1, ~negative_values] = error[~negative_values]
            ax1.errorbar(
                avg_pred,
                avg_true,
                yerr=ygaps,
                fmt=" ",
                color=color_gaps,
                lw=4,
                capsize=5,
                capthick=1,
                zorder=10,
            )

        if sample_proportion > 0:
            idx = np.random.choice(labels.shape[0], int(sample_proportion * labels.shape[0]))
            ax1.scatter(
                score[idx],
                labels[idx],
                marker="|",
                s=100,
                alpha=0.2,
                color=color_list[j],
            )

        ax1.set_xlim((0, 1))
        ax1.xaxis.set_major_locator(MultipleLocator(0.20))
        ax1.xaxis.set_minor_locator(MultipleLocator(0.10))
        ax1.set_ylim((0, 1))
        ax1.yaxis.set_major_locator(MultipleLocator(0.20))
        ax1.yaxis.set_minor_locator(MultipleLocator(0.10))
        if not show_histogram or overlay_histogram:
            ax1.set_xlabel("Average score")
        elif show_histogram:
            ax1.set_xticklabels([])
        ax1.set_ylabel("Fraction of positives")
        ax1.grid(which="both")
        # ax1.set_aspect(1)
        ax1.set_axisbelow(True)

        if show_histogram:
            if overlay_histogram:
                ax2 = ax1.twinx()
            else:
                divider = make_axes_locatable(ax1)
                ax2 = divider.append_axes("bottom", size="20%", pad=0.1, sharex=ax1)

            # ax2 = fig.add_subplot(spec[1], label='{}'.format(i))
            for j, score in enumerate(scores_list):
                # lines = ax1.get_lines()
                # ax2.set_xticklabels([])

                name = legend[j] if legend else None
                if n_scores > 1:
                    kwargs = {"histtype": "step", "edgecolor": color_list[j]}
                else:
                    kwargs = {"histtype": "bar", "edgecolor": "black", "color": color_list[j]}
                if overlay_histogram:
                    kwargs = {**kwargs, "alpha": 0.4}

                ax2.hist(score, range=(0, 1), bins=bins, label=name, lw=1, **kwargs)
                ax2.set_xlim((0, 1))
                ax2.set_xlabel("Average score")
                ax2.yaxis.set_major_locator(MaxNLocator(integer=True, prune="upper", nbins=3))
            ax2.set_ylabel("Count")
            if not overlay_histogram:
                ytickloc = ax2.get_yticks()
                ax2.yaxis.set_major_locator(mticker.FixedLocator(ytickloc))
                yticklabels = [f"{value:0.0f}" for value in ytickloc]
                ax2.set_yticklabels(labels=yticklabels, fontdict={"verticalalignment": "top"})
                ax2.grid(True, which="both")
                ax2.set_axisbelow(True)
            if invert_histogram:
                ylim = ax2.get_ylim()
                ax2.set_ylim(reversed(ylim))

    if legend is not None:
        lines, labels = fig.axes[0].get_legend_handles_labels()
        fig.legend(
            lines,
            labels,
            loc="upper center",
            bbox_to_anchor=(0, 0, 1, 1),
            bbox_transform=fig.transFigure,
            ncol=6,
        )

    fig.align_labels()
    return fig


def plot_roc_curve(labels, scores, legend=None, color_list=None, fmt="-", fig=None, ax=None):
    """Plot the ROC curve of the given scores and true labels.

    Parameters
    ----------
    labels : array (n_samples, )
        Labels indicating the true class.
    scores : array (n_samples,) or list of matrices
        Output probability scores for one or several methods.
    legend : list of strings or None
        Text to use for the legend.
    color_list : list of strings or None
        List of string colors indicating the color of each method.
    fmt : string (default: 's-')
        Format of the lines following the matplotlib.pyplot.plot standard.
    fig : matplotlib.pyplot.Figure or None
        Figure to use for the plots, if None a new figure is created.
    ax : matplotlib.pyplot.Axes or None
        Axes to use for the plots, if None a new axes is created.

    Returns
    -------
    fig : matplotlib.pyplot.figure
        Figure with the ROC curve
    """
    if isinstance(scores, list):
        scores_list = scores
    else:
        scores_list = [
            scores,
        ]

    if color_list is None:
        color_list = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    if fig is None:
        fig = plt.figure(figsize=(4, 4))
    spec = gridspec.GridSpec(ncols=1, nrows=1, hspace=0.04, left=0.15)

    if ax is None:
        ax = fig.add_subplot(spec[0])

    for j, score in enumerate(scores_list):
        fpr, tpr, _ = roc_curve(labels, score)
        roc_auc = auc(fpr, tpr)
        name = f"{legend[j]} (AUC = {roc_auc:.2f})" if legend else None

        ax.plot(fpr, tpr, fmt, linewidth=1, label=name, color=color_list[j])

    # reference line, legends, and axis labels
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray")
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_locator(MultipleLocator(0.20))
    ax.xaxis.set_minor_locator(MultipleLocator(0.10))
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_locator(MultipleLocator(0.20))
    ax.yaxis.set_minor_locator(MultipleLocator(0.10))
    ax.grid(which="both")

    # plt.gca().xaxis.set_ticks_position('none')
    # plt.gca().yaxis.set_ticks_position('none')

    if legend is not None:
        lines, labels = fig.axes[0].get_legend_handles_labels()
        fig.legend(
            lines,
            labels,
            loc="lower right",
            bbox_to_anchor=(0, 0, 1, 1),
            bbox_transform=fig.transFigure,
            ncol=6,
        )

    fig.align_labels()
    return fig


def _check_binary_probabilistic_predictions(y_true, y_prob):
    """Check that y_true is binary and y_prob contains valid probabilities."""
    # convert to 1D numpy array
    y_true = column_or_1d(y_true)
    y_prob = column_or_1d(y_prob)

    # check equal length
    check_consistent_length(y_true, y_prob)

    if y_prob.min() < 0 or y_prob.max() > 1:
        warnings.warn(
            "y_prob has values outside [0, 1] and normalize is set to False. "
            "Probalities outside [0, 1] will be clipped."
        )
        y_prob = np.clip(y_prob, a_min=0, a_max=1)

    # check if binary classification
    labels = np.unique(y_true)
    if len(labels) != 2:
        raise ValueError("Only binary classification is supported. Provided labels %s." % labels)

    return label_binarize(y_true, classes=labels)[:, 0], y_prob


def expected_calibration_error(y_true, y_prob, n_bins=5, strategy="uniform"):
    """Compute the Expected Calibration Error (ECE).

    This method implements equation (3) in [1], as well as the ACE variant in [2].
    In this equation the probability of the decided label being correct is
    used to estimate the calibration property of the predictor.

    Note: a trade-off exist between using a small number of `n_bins` and the
    estimation reliability of the ECE.  In particular, this method may produce
    unreliable ECE estimates in case there are few samples available in some bins.

    Parameters
    ----------
    y_true : array, shape (n_samples,)
        True targets.
    y_prob : array, shape (n_samples,)
        Probabilities of the positive class.
    n_bins : int, default=5
        Number of bins to discretize the [0, 1] interval. A bigger number
        requires more data. Bins with no samples (i.e. without
        corresponding values in `y_prob`) will not be returned, thus the
        returned arrays may have less than `n_bins` values.
    strategy : {'uniform', 'quantile'}, default='uniform'
        Strategy used to define the widths of the bins.
        uniform
            The bins have identical widths. This corresponds to the ECE formula.
        quantile
            The bins have the same number of samples and depend on `y_prob`. This
            corresponds to the ACE formula.

    Returns
    -------
    ece : float
       The expected calibration error.

    Raises
    ------
    ValueError
        If strategy is not 'uniform' or 'quantile'.

    References
    ----------
    .. [1] Chuan Guo, Geoff Pleiss, Yu Sun, Kilian Q. Weinberger, On
           Calibration of Modern Neural Networks. Proceedings of the 34th
           International Conference on Machine Learning (ICML 2017).
           arXiv:1706.04599 https://arxiv.org/pdf/1706.04599.pdf
    .. [2] Nixon, Jeremy, et al., Measuring calibration in deep learning.
           arXiv:1904.01685 https://arxiv.org/abs/1904.01685
    """
    y_true, y_prob = _check_binary_probabilistic_predictions(y_true, y_prob)

    if strategy == "quantile":  # Determine bin edges by distribution of data
        quantiles = np.linspace(0, 1, n_bins + 1)
        bins = np.percentile(y_prob, quantiles * 100)
        bins[0] = 0 - 1e-8
        bins[-1] = 1 + 1e-8
    elif strategy == "uniform":
        bins = np.linspace(0.0, 1.0 + 1e-8, n_bins + 1)
    else:
        raise ValueError(
            "Invalid entry to 'strategy' input. Strategy "
            "must be either 'quantile' or 'uniform'."
        )

    n = y_prob.shape[0]
    accs, confs, counts = _reliability(y_true, y_prob, bins)
    return np.sum(counts * np.abs(accs - confs) / n)


def _reliability(y_true, y_prob, bins):
    n_bins = len(bins) - 1
    accs = np.zeros(n_bins)
    confs = np.zeros(n_bins)
    counts = np.zeros(n_bins)
    for m in range(n_bins):
        low = bins[m]
        high = bins[m + 1]

        where_in_bin = (low <= y_prob) & (y_prob < high)
        if where_in_bin.sum() > 0:
            accs[m] = (
                np.sum((y_prob[where_in_bin] >= 0.5) == y_true[where_in_bin]) / where_in_bin.sum()
            )
            confs[m] = np.mean(np.maximum(y_prob[where_in_bin], 1 - y_prob[where_in_bin]))
            counts[m] = where_in_bin.sum()

    return accs, confs, counts


def calibration_curve(y_true, y_prob, bins=10, bin_strategy="uniform"):
    """Compute true and predicted probabilities for a calibration curve.

    Parameters
    ----------
    y_true : array (n_samples, )
        Labels indicating the true class.
    y_prob : array (n_samples, )
        Output probability scores.
    bins : int or list of floats
        Number of bins to create in the scores' space, or list of bin
        boundaries. More bins require more data.
    bin_strategy : {'uniform', 'quantile'}, default='uniform'
        Strategy used to define the widths of the bins.

        uniform
            The bins have identical widths.
        quantile
            The bins have the same number of samples and depend on `y_prob`.

    Returns
    -------
    avg_true : array, shape (n_bins,)
        The true probability in each bin (fraction of positives).

    avg_pred : array, shape (n_bins,)
        The mean predicted probability in each bin.

    bin_true : array, shape (n_bins,)
        Number of true samples in each bin.

    bin_total : array, shape (n_bins,)
        Number of samples in each bin.

    Raises
    ------
    ValueError
        If `bins` is not an integer or a list of floats.

    References
    ----------
    .. [1] Alexandru Niculescu-Mizil and Rich Caruana (2005) Predicting Good
           Probabilities With Supervised Learning, in Proceedings of the 22nd
           International Conference on Machine Learning (ICML).
           See section 4 (Qualitative Analysis of Predictions).
    """
    y_true, y_prob = _check_binary_probabilistic_predictions(y_true, y_prob)

    if isinstance(bins, int):
        n_bins = bins
        if bin_strategy == "quantile":  # Determine bin edges by distribution of data
            quantiles = np.linspace(0, 1, n_bins + 1)
            bins = np.percentile(y_prob, quantiles * 100)
            bins[0] = 0 - 1e-8
            bins[-1] = 1 + 1e-8
        elif bin_strategy == "uniform":
            bins = np.linspace(0, 1 + 1e-8, n_bins + 1)
        else:
            raise ValueError(
                "Invalid entry to 'strategy' input. Strategy "
                "must be either 'quantile' or 'uniform'."
            )
    elif isinstance(bins, list) or isinstance(bins, np.ndarray):
        n_bins = len(bins) - 1
        bins = np.array(bins)
        if bins[0] == 0.0:
            bins[0] = 0 - 1e-8
        if bins[-1] == 1.0:
            bins[-1] = 1 + 1e-8
    else:
        raise ValueError(
            "Invalid entry to 'bins' input. The must be either "
            "a list of bin boundaries or the number of bins."
        )

    bin_idx = np.digitize(y_prob, bins) - 1

    bin_true = np.bincount(bin_idx, weights=y_true, minlength=n_bins)
    bin_pred = np.bincount(bin_idx, weights=y_prob, minlength=n_bins)
    bin_total = np.bincount(bin_idx, minlength=n_bins)

    zero_idx = bin_total == 0
    avg_true = np.empty(bin_total.shape[0])
    avg_true.fill(np.nan)
    avg_true[~zero_idx] = np.divide(bin_true[~zero_idx], bin_total[~zero_idx])
    avg_pred = np.empty(bin_total.shape[0])
    avg_pred.fill(np.nan)
    avg_pred[~zero_idx] = np.divide(bin_pred[~zero_idx], bin_total[~zero_idx])
    return avg_true, avg_pred, bin_true, bin_total


def bayesian_calibration_curve(y_true, y_prob, n_bins=100):
    """Compute true and predicted probabilities for a calibration curve using kernel density estimation.

    Parameters
    ----------
    y_true : array-like of shape (n_samples,)
        True targets.
    y_prob : array-like of shape (n_samples,)
        Probabilities of the positive class.
    n_bins : float, default=100
        Number of bins to discretize the [0, 1] interval. A bigger number
        requires more data.

    Returns
    -------
    avg_true : array, shape (n_bins,)
        The true probability in each bin (fraction of positives).

    avg_pred : array, shape (n_bins,)
        The mean predicted probability in each bin.

    bin_true : array, shape (n_bins,)
        Number of true samples in each bin.

    bin_total : array, shape (n_bins,)
        Number of samples in each bin.
    """
    y_true, y_prob = _check_binary_probabilistic_predictions(y_true, y_prob)
    y_true = y_true.astype(bool)

    bandwidth = 1 / n_bins
    kde_pos = KernelDensity(kernel="gaussian", bandwidth=bandwidth).fit(
        (y_prob[y_true])[:, np.newaxis]
    )
    kde_total = KernelDensity(kernel="gaussian", bandwidth=bandwidth).fit(y_prob[:, np.newaxis])
    sample_probabilities = np.linspace(0.01, 0.99, 99)
    number_density_offense_won = np.exp(
        kde_pos.score_samples(sample_probabilities[:, np.newaxis])
    ) * np.sum(y_true)
    number_density_total = np.exp(
        kde_total.score_samples(sample_probabilities[:, np.newaxis])
    ) * len(y_true)
    number_pos = number_density_offense_won * np.sum(y_true) / np.sum(number_density_offense_won)
    number_total = number_density_total * len(y_true) / np.sum(number_density_total)
    predicted_pos_percents = np.nan_to_num(number_pos / number_total, 1)

    return (
        predicted_pos_percents,
        sample_probabilities,
        number_pos,
        number_total,
    )


def max_deviation(sample_probabilities, predicted_pos_percents):
    """Compute the largest discrepancy between the model and expectation."""
    abs_deviations = np.abs(predicted_pos_percents - sample_probabilities)
    return np.max(abs_deviations)


def residual_area(sample_probabilities, predicted_pos_percents):
    """Compute the total area under the curve of |predicted prob - expected prob|."""
    abs_deviations = np.abs(predicted_pos_percents - sample_probabilities)
    return integrate.trapz(abs_deviations, sample_probabilities)
