from textwrap import wrap
import matplotlib.pyplot as plt
import numpy as np

color_list = [
    "cornflowerblue",
    "gold",
    "deeppink",
    "orange",
    "blueviolet",
    "seagreen",
    "black",
]
hatch_list = ["o", "x", "/", ".", "*", "-", "\\"]

linestyle_list = ["solid", "dashed", "dashdot", "dotted"]
markertype_list = ["o", "^", "x", "x", "|"]
def plot_cdf(
    raw_data,
    file_name,
    linelabels,
    x_label,
    y_label="CDF",
    log_switch=False,
    rotate_xaxis=False,
    ylim_low=0,
    xlim=None,
    xlim_bottom=None,
    fontsize=15,
    legend_font=15,
    loc=2,
    title=None,
    enable_abs=False,
    group_size=1,
):
    _fontsize = fontsize
    fig = plt.figure(figsize=(6, 4))  # 2.5 inch for 1/3 double column width
    ax = fig.add_subplot(111)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    ax.tick_params(axis="y", direction="in")
    ax.tick_params(axis="x", direction="in")
    if log_switch:
        ax.set_xscale("log")

    plt.ylabel(y_label, fontsize=_fontsize)
    plt.xlabel(x_label, fontsize=_fontsize)
    linelabels = ["\n".join(wrap(l, 30)) for l in linelabels]
    for i in range(len(raw_data)):
        data = raw_data[i]
        data = data[~np.isnan(data)]
        if len(data) == 0:
            continue
        if enable_abs:
            data = abs(data)
        # data=random.sample(data,min(1e6,len(data)))
        data_size = len(data)
        # data=list(filter(lambda score: 0<=score < std_val, data))
        # Set bins edges
        data_set = sorted(set(data))
        bins = np.append(data_set, data_set[-1] + 1)

        # Use the histogram function to bin the data
        counts, bin_edges = np.histogram(data, bins=bins, density=False)

        counts = counts.astype(float) / data_size

        # Find the cdf
        cdf = np.cumsum(counts)

        # Plot the cdf
        if i < len(linelabels):
            plt.plot(
                bin_edges[0:-1],
                cdf,
                linestyle=linestyle_list[(i // group_size) % len(linestyle_list)],
                color=color_list[(i % group_size) % len(color_list)],
                label=linelabels[i],
                linewidth=3,
            )
        else:
            plt.plot(
                bin_edges[0:-1],
                cdf,
                linestyle=linestyle_list[(i // group_size) % len(linestyle_list)],
                color=color_list[(i % group_size) % len(color_list)],
                linewidth=3,
            )

    legend_properties = {"size": legend_font}
    plt.legend(
        prop=legend_properties,
        frameon=False,
        loc=loc,
    )

    plt.ylim((ylim_low, 1))
    if xlim_bottom:
        plt.xlim(left=xlim_bottom)
    if xlim:
        plt.xlim(right=xlim)
    # plt.tight_layout()
    # plt.tight_layout(pad=0.5, w_pad=0.04, h_pad=0.01)
    plt.yticks(fontsize=_fontsize)
    plt.xticks(fontsize=_fontsize)
    # plt.grid(True)
    if rotate_xaxis:
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment="right")
    if title:
        plt.title(title, fontsize=_fontsize - 5)
    # plt.savefig(file_name, bbox_inches="tight", pad_inches=0)

def recover_data(sampling_percentiles, sampled_data,target_percentiles,method='linear'):
    recovered_data = []

    for percentile in target_percentiles:
        # Find the two nearest percentiles in the sampled data
        lower_percentile = max(filter(lambda x: x <= percentile, sampling_percentiles), default=0)
        upper_percentile = min(filter(lambda x: x >= percentile, sampling_percentiles), default=100)

        # Retrieve corresponding values from the sampled data
        lower_index = np.where(sampling_percentiles == lower_percentile)[0][0] if lower_percentile in sampling_percentiles else 0
        upper_index = np.where(sampling_percentiles == upper_percentile)[0][0] if upper_percentile in sampling_percentiles else -1

        lower_value = sampled_data[lower_index]
        upper_value = sampled_data[upper_index]

        if method=='linear':
            # Interpolate to recover the original data
            recovered_value = np.interp(percentile, [lower_percentile, upper_percentile], [lower_value, upper_value])
        elif method=='nearest':
            recovered_value=lower_value if percentile-lower_percentile<upper_percentile-percentile else upper_value
        elif method=='higher':
            recovered_value=upper_value
        # Append the recovered value to the list
        recovered_data.append(recovered_value)

    return recovered_data