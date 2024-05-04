from textwrap import wrap
import matplotlib.pyplot as plt
import numpy as np

color_list = [
    "cornflowerblue",
    "orange",
    "deeppink",
    "black",
    "blueviolet",
    "seagreen",
]
hatch_list = ["o", "x", "/", ".", "*", "-", "\\"]
linestyle_list = ["-", "-.", ":","--"]
markertype_list = ["o", "^","x", "x","|"]

def plot_cdf(
    raw_data,
    file_name,
    linelabels,
    x_label,
    y_label="CDF (%)",
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
    fig = plt.figure(figsize=(5.2, 3))  # 2.5 inch for 1/3 double column width
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
        data_size = len(data)
        # Set bins edges
        data_set = sorted(set(data))
        bins = np.append(data_set, data_set[-1] + 1)

        # Use the histogram function to bin the data
        counts, bin_edges = np.histogram(data, bins=bins, density=False)

        counts = counts.astype(float) / data_size

        # Find the cdf
        cdf = np.cumsum(counts)
        cdf=100 * cdf / cdf[-1]
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

    plt.ylim((ylim_low, 100))
    if xlim_bottom:
        plt.xlim(left=xlim_bottom)
    if xlim:
        plt.xlim(right=xlim)
    plt.yticks(fontsize=_fontsize)
    plt.xticks(fontsize=_fontsize)
    if rotate_xaxis:
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment="right")
    if title:
        plt.title(title, fontsize=_fontsize - 5)
    if file_name:
        plt.savefig(file_name, bbox_inches="tight", pad_inches=0)
        
def plot_bar(
    datas,
    xs,
    linelabels=None,
    label=None,
    y_label="CDF",
    name="ss",
    log_switch=False,
    fontsize=15,
    ylim=None,
    ylim_bottom=None,
    title=None,
    rotate_xaxis=False,
):
    _fontsize = fontsize
    fig = plt.figure(figsize=(5.2, 4))  # 2.5 inch for 1/3 double column width
    ax = fig.add_subplot(111)
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    
    ax.tick_params(axis="y", direction="in")
    ax.tick_params(axis="x", direction="in")
    
    plt.ylabel(y_label, fontsize=_fontsize)
    plt.xlabel(label, fontsize=_fontsize)

    for i in range(len(datas)):
        tmp_data = datas[i]
        X = np.arange(0, 1, 1 / len(tmp_data))
        width = 0.5 / len(tmp_data)
        ax.bar(X + 0.2, tmp_data, color=color_list, width=width, label=linelabels)

    legend_properties = {"size": 15}
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=color_list[ii]) for ii in range(len(linelabels))
    ]
    plt.legend(handles, linelabels, ncol=1, prop=legend_properties, frameon=False)

    if ylim_bottom:
        plt.ylim(left=ylim_bottom)
    if ylim:
        plt.ylim(right=ylim)
        
    if log_switch:
        ax.set_yscale("log")

    plt.tight_layout()

    plt.tight_layout(pad=0.5, w_pad=0.01, h_pad=0.01)
    plt.yticks(fontsize=_fontsize)
    plt.xticks(fontsize=_fontsize)
    if rotate_xaxis:
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment="right")
    if title:
        plt.title(title, fontsize=_fontsize - 5)
    # plt.savefig(name, bbox_inches="tight", pad_inches=0)

def plot_scatter(
    raw_data,
    file_name,
    linelabels,
    x_label,
    marker_size=50,
    enable_legend=False,
    y_label="Value",
    log_switch=False,
    rotate_xaxis=False,
    ylim=None,
    ylim_bottom=None,
    xlim=None,
    xlim_bottom=None,
    fontsize=15,
    legend_font=15,
    loc=2,
    title=None,
    frameon=False,
    ncol=1,
    text_info=None,
    text_offset=None  # Default text offset
):
    _fontsize = fontsize
    fig = plt.figure(figsize=(5.2, 2.5))  # Figure size
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

    for i, data in enumerate(raw_data):
        # Assuming data is a list of tuples/lists with (x, y) format
        print(data.shape)
        x, y = zip(*data)  # Extracting x and y coordinates
        
        # Plotting each dataset as a scatter plot
        plt.scatter(x, y, label=linelabels[i] if i < len(linelabels) else None, color=color_list[i % len(color_list)], marker=markertype_list[i % len(markertype_list)], s=marker_size)
        
        if text_info and i==0:
            for j, (x_val, y_val) in enumerate(data):
                plt.text(x_val + text_offset[j][0], y_val + text_offset[j][1], text_info[j], fontsize=fontsize-4)
                
    if enable_legend and loc is not None:            
        legend_properties = {"size": legend_font}
        plt.legend(
            prop=legend_properties,
            frameon=frameon,
            loc=loc,
            ncol=ncol,
        )

    if ylim:
        plt.ylim(top=ylim)
    if ylim_bottom:
        plt.ylim(bottom=ylim_bottom)
    if xlim:
        plt.xlim(right=xlim)
    if xlim_bottom:
        plt.xlim(left=xlim_bottom)
    plt.yticks(fontsize=_fontsize)
    plt.xticks(fontsize=_fontsize)
    if rotate_xaxis:
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment="right")
    if title:
        plt.title(title, fontsize=_fontsize - 5)
    if file_name:
        plt.savefig(file_name, bbox_inches="tight", pad_inches=0)