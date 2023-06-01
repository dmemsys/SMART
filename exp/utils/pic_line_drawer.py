import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from pathlib import Path
import numpy as np


class LineDrawer(object):

    def __init__(self, pic_dir: str):
        Path(pic_dir).mkdir(exist_ok=True)
        self.pic_dir = pic_dir


    def __load_default_style(self):
        # line
        self.lineStyleDict = {
            'Sherman (2MN)'    : (0, (5, 3)),
            'Sherman (1MN)'    : (0, ()),
            'ART (2MN)'        : (0, (5, 3)),
            'ART (1MN)'        : (0, ()),
            'ART'            : (0, ()),
            'Sherman'        : (0, ()),
            'SMART'          : (0, ()),

            'Throughput': (0, ()),
            'P99 Latency': (0, ()),

            'RDMA_READ' : (0, ()),
            'RDMA_WRITE': (0, ()),
            'RDMA_CAS'  : (0, ()),
        }
        self.lineColorDict = {
            'Sherman (2MN)'    : '#4575B5',
            'Sherman (1MN)'    : '#4575B5',
            'ART (2MN)'        : '#D63026',
            'ART (1MN)'        : '#D63026',
            'ART'            : '#D63026',
            'Sherman'        : '#4575B5',
            'SMART'          : '#82B366',

            'Throughput': '#4575B5',
            'P99 Latency': '#D63026',

            'RDMA_READ' : '#D63026',
            'RDMA_WRITE': '#D63026',
            'RDMA_CAS'  : '#82B366',
        }
        self.lineMarkerDict = {
            'Sherman (2MN)'    : 'x',
            'Sherman (1MN)'    : 'x',
            'ART (2MN)'        : 'o',
            'ART (1MN)'        : 'o',
            'ART'            : 'o',
            'Sherman'        : 'x',
            'SMART'          : 's',

            'Throughput': 'x',
            'P99 Latency': '^',

            'RDMA_READ' : 'x',
            'RDMA_WRITE': 'x',
            'RDMA_CAS'  : '^',
        }
        self.zorderDict = {
            'Sherman (2MN)'    : 1100,
            'Sherman (1MN)'    : 1100,
            'ART (2MN)'        : 1200,
            'ART (1MN)'        : 1200,
            'ART'            : 1200,
            'Sherman'        : 1150,
            'SMART'          : 1250,

            'Throughput': 1000,
            'P99 Latency': 1000,

            'RDMA_READ' : 1000,
            'RDMA_WRITE': 1000,
            'RDMA_CAS'  : 1100,
        }
        # size
        self.figsize=(4, 2.5)
        self.font_size = 15
        self.legend_size = 15
        self.tick_size = 14
        self.linewidth = 0.8
        self.markersize = 6
        # grid
        self.grid_type = {'axis': 'y', 'lw': 0.3}
        self.y_major_num = self.x_major_num = 0
        self.grid_minor = False
        # edge
        self.hide_half_edge = False
        self.hide_ylabel = False
        self.clip_on = True
        # legend
        self.legend_location = ''
        self.legend_anchor = ()
        self.legendL_anchor = self.legendR_anchor = ()
        self.legend_ncol = 1
        # tick
        self.x_ticklabel = False
        self.y_lim = self.x_lim = ()
        self.ylim = self.xlim = ()
        self.yL_lim = self.yR_lim = ()
        self.x_tick = self.y_tick = []
        self.yL_tick = self.yR_tick = []
        self.yscale = ''
        self.yfloat = False
        # label
        self.x_label = ''
        self.y_label = ''
        self.yL_label = ''
        self.yR_label = ''
        # func
        self.aux_plt_func = None
        self.annotation_func = None


    def plot_with_one_ax(self, data: dict, fig_name: str, custom_style: dict = {}, method_legend: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        if self.hide_half_edge:
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)
        legend_handles = []
        legend_labels  = []
        if not method_legend:
            method_legend = {method: method for method in data['methods']}

        X_data = data['X_data']
        Y_data = data['Y_data']
        for method in data['methods']:
            l, = ax.plot(X_data[method] if isinstance(X_data, dict) else X_data,
                         Y_data[method],
                         linestyle=self.lineStyleDict[method],
                         color=self.lineColorDict[method],
                         marker=self.lineMarkerDict[method],
                         markerfacecolor='none',
                         mew=self.linewidth,
                         clip_on=self.clip_on,
                         linewidth=self.linewidth,
                         markersize=self.markersize + 1.5 if self.lineMarkerDict[method] == '+' else
                                    self.markersize + 0.5 if self.lineMarkerDict[method] == 'x' else self.markersize,
                         zorder=self.zorderDict[method])
            if method in method_legend:
                legend_handles.append(l)
                legend_labels.append(method_legend[method])
        ax.set_xlabel(self.x_label, fontsize=self.font_size)
        if not self.hide_ylabel:
            ax.set_ylabel(self.y_label, fontsize=self.font_size)
        if self.yscale:
            ax.set_yscale(self.yscale)
        if self.yfloat:
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        if self.ylim:
            ytick       = list(np.linspace(self.ylim[0], self.ylim[1], self.y_major_num))
            ytick_minor = list(np.linspace(self.ylim[0], self.ylim[1], 0 if self.grid_minor is False else self.y_major_num * 2 - 1))
            ax.set_yticks(ytick)
            ax.set_yticks(ytick_minor, minor=True)
        if self.xlim:
            xtick       = list(np.linspace(self.xlim[0], self.xlim[1], self.x_major_num))
            xtick_minor = list(np.linspace(self.xlim[0], self.xlim[1], 0 if self.grid_minor is False else self.x_major_num * 2 - 1))
            ax.set_xticks(xtick)
            ax.set_xticks(xtick_minor, minor=True)
        if self.y_tick:
            ax.set_yticks(self.y_tick)
        if self.x_tick:
            ax.set_xticks(self.x_tick)
            if self.x_ticklabel:
                ax.set_xticklabels(self.x_ticklabel, fontsize=self.tick_size)
        if self.y_lim:  # x, y limitation, use when xlim/ylim is not enough
            ax.set_ylim(*self.y_lim)
        if self.x_lim:
            ax.set_xlim(*self.x_lim)
        ax.tick_params(labelsize=self.tick_size)
        # ax.set_xscale('log')
        if self.annotation_func:
            self.annotation_func(ax)
        ax.grid(color='#dbdbdb', **self.grid_type, which='both' if self.grid_minor else 'major', zorder=0)
        if self.legend_location or self.legend_anchor:
            if self.legend_anchor:
                ax.legend(legend_handles, legend_labels, fontsize=self.legend_size, bbox_to_anchor=self.legend_anchor, frameon=False, ncol=self.legend_ncol)
            else:
                ax.legend(legend_handles, legend_labels, fontsize=self.legend_size, loc=self.legend_location, frameon=False, ncol=self.legend_ncol) # labelspacing=0.1
        if self.aux_plt_func:
            self.aux_plt_func(ax)
        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()


    def plot_with_two_ax(self, data: dict, fig_name: str, custom_style: dict = {}, method_legend: dict = {}, method_ax: dict = {}):
        self.__load_default_style()
        # load custom style
        for k, v in custom_style.items():
            setattr(self, k, v)

        fig, ax_L = plt.subplots(1, 1, figsize=self.figsize, dpi=300)
        ax_R = ax_L.twinx()
        # legend_list = []
        if not method_legend:
            method_legend = {method: method for method in data['methods']}
        if not method_ax:
            method_ax = dict(zip(data['methods'], ['left', 'right']))

        X_data = data['X_data']
        Y_data = data['Y_data']
        for method in data['methods']:
            ax = ax_L if method_ax[method] == 'left' else ax_R
            ax.plot(X_data[method] if isinstance(X_data, dict) else X_data,
                    Y_data[method],
                    label=method_legend[method],
                    linestyle=self.lineStyleDict[method],
                    color=self.lineColorDict[method],
                    marker=self.lineMarkerDict[method],
                    markerfacecolor='none',
                    clip_on=False,
                    linewidth=self.linewidth,
                    markersize=self.markersize + 1.5 if self.lineMarkerDict[method] == '+' else
                               self.markersize + 0.5 if self.lineMarkerDict[method] == 'x' else self.markersize)
            # if method in method_legend:
            #     legend_list.append(method_legend[method])
        ax_L.set_xlabel(self.x_label, fontsize=self.font_size)
        ax_L.set_ylabel(self.yL_label, fontsize=self.font_size)
        ax_R.set_ylabel(self.yR_label, fontsize=self.font_size)
        if self.yL_tick:
            ax_L.set_yticks(self.yL_tick)
            ax_L.set_yticklabels(self.yL_tick, fontsize=self.tick_size)
        if self.yR_tick:
            ax_R.set_yticks(self.yR_tick)
            ax_R.set_yticklabels(self.yR_tick, fontsize=self.tick_size)
        if self.yfloat:
            ax_L.yaxis.set_major_formatter(FormatStrFormatter('%.1f'))
        if self.x_tick:
            ax_L.set_xticks(self.x_tick)
            ax_L.set_xticklabels(self.x_tick, fontsize=self.tick_size)
        if self.yL_lim:
            ax_L.set_ylim(*self.yL_lim)
        if self.yR_lim:
            ax_R.set_ylim(*self.yR_lim)
        if self.x_lim:
            ax.set_xlim(*self.x_lim)
        ax_L.grid(color='#dbdbdb', **self.grid_type, which='both', zorder=0)
        ax_L.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendL_anchor, frameon=False, ncol=self.legend_ncol)
        ax_R.legend(fontsize=self.legend_size, bbox_to_anchor=self.legendR_anchor, frameon=False, ncol=self.legend_ncol)
        if self.aux_plt_func:
            self.aux_plt_func(ax)
        plt.savefig(str(Path(self.pic_dir) / fig_name), format='pdf', bbox_inches='tight')
        plt.close()