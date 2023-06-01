from pathlib import Path
import json

from utils.pic_line_drawer import LineDrawer
from utils.pic_bar_drawer import BarDrawer
from utils.color_printer import print_OK


class PicGenerator(object):

    def __init__(self, data_path: str, style_path: str):
        self.__data_path = data_path
        self.__style_path = style_path
        self.__ld = LineDrawer(data_path)
        self.__bd = BarDrawer(data_path)
        self.__figs_type = {
            '3a' : 'line_one_ax', '3b' : 'bar_one_ax'   , '3c' : 'line_one_ax'  , '3d' : 'line_one_ax',
            '4a' : 'line_two_ax', '4b' : 'bar_two_ax'   , '4c' : 'line_one_ax'  , '4d' : 'line_two_ax',
            '11a': 'line_one_ax', '11b': 'line_one_ax'  , '11c': 'line_one_ax'  , '11d': 'line_one_ax', '11e': 'line_one_ax'  ,
            '12a': 'line_one_ax', '12b': 'line_one_ax'  , '12c': 'line_one_ax'  , '12d': 'line_one_ax', '12e': 'line_one_ax'  ,
            '13' : 'line_one_ax', '14' : 'bar_with_line', '15' : 'bar_with_line', '16' : 'bar_two_ax' , '17' : 'bar_with_line',
            '18a': 'line_one_ax', '18b': 'line_one_ax'  , '18c': 'line_one_ax'
        }
        self.__axhlineColor = '#00007c'
        self.__linewidth = 0.8
        self.__font_size = 15


    def __aux_plt(self, ax):
        ax.axhline(y=45, ls=(0, (5, 3)), c=self.__axhlineColor, lw=self.__linewidth)
        ax.text(200, 47, 'IOPS Upper Bound', fontsize=self.__font_size-1, color=self.__axhlineColor)
        ax.axhline(y=7, ls=(0, (5, 3)), c=self.__axhlineColor, lw=self.__linewidth)
        ax.text(120, 9, 'Bandwidth Bottleneck', fontsize=self.__font_size-1, color=self.__axhlineColor)


    def __annotation(self, ax):
        ax.annotate('', xy=(4, 14), xytext=(40, 14), arrowprops=dict(arrowstyle="<->", color=self.__axhlineColor, ls=(0, (5, 3))), fontsize=self.__font_size-1)
        ax.text(12.5, 16, '4M vs. 40M', fontsize=self.__font_size-1, color=self.__axhlineColor)


    def generate(self, fig_num: str):
        fig_name = f"fig_{fig_num}.pdf"
        fig_type = self.__figs_type[fig_num]

        # load data
        with (Path(self.__data_path) / f'fig_{fig_num}.json').open(mode='r') as f:
            data = json.load(f)
        # load style
        with (Path(self.__style_path) / f'fig_{fig_num}.json').open(mode='r') as f:
            custom_style = json.load(f)
        # load func
        if fig_num == '3c':
            custom_style['aux_plt_func'] = self.__aux_plt
        if fig_num == '3d':
            custom_style['aux_plt_func'] = self.__annotation

        # draw
        if fig_type == 'line_one_ax':
            self.__ld.plot_with_one_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'line_two_ax':
            self.__ld.plot_with_two_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_one_ax':
            self.__bd.plot_with_one_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_two_ax':
            self.__bd.plot_with_two_ax(data, fig_name, custom_style=custom_style)
        elif fig_type == 'bar_with_line':
            self.__bd.plot_with_line(data, fig_name, custom_style=custom_style)

        print_OK(f"Draw {fig_name} done!")
