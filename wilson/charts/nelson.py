import pyspark.sql.functions as func
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window

import wilson.charts.utils as utils


class NelsonRules:
    """Nelson rules for six sigma analysis.

    More about the rules:
    - http://leansixsigmadefinition.com/glossary/nelson-rules/
    - https://en.wikipedia.org/w/index.php?title=Nelson_rules
    """

    def __init__(self, timecol='timestamp'):
        self.timecol = timecol
        self.rules = {
            'nelson_1': self.rule1,
            'nelson_2': self.rule2,
            'nelson_3': self.rule3,
            'nelson_4': self.rule4,
            'nelson_5': self.rule5,
            'nelson_6': self.rule6,
            'nelson_7': self.rule7,
            'nelson_8': self.rule8,
        }

    def apply(self, df, colnames):
        if not isinstance(colnames, list):
            colnames = [colnames]

        for colname in colnames:
            mean, std = utils.compute_stats(df, colname)
            df = (df
                  .withColumn('{}_mean'.format(colname), lit(mean))
                  .withColumn('{}_std'.format(colname), lit(std)))

            for rulename, rule in self.rules.items():
                df = df.withColumn(f'{colname}_{rulename}', rule(colname))

        return df

    def rule1(self, colname):
        """Nelson rule 1
        control limit breach - 1 point is more than 3 stddev from mean
        """
        column = col(colname)
        lower, upper = utils.get_control_range(colname, 3)

        return ~column.between(lower, upper)

    def rule2(self, colname):
        """Nelson rule 2
        7 or more points are the same side of the mean (should be 9 points)
        """
        column = col(colname)
        window = utils.generate_window(self.timecol, (-6, 0))

        mean = col('{}_mean'.format(colname))

        all_above = utils.over(column, mean, window) == 7
        all_below = utils.under(column, mean, window) == 7

        return all_above | all_below

    def rule3(self, colname):
        """Nelson rule 3
        7 or more points are increasing / decreasing (should be 6 points)
        """
        column = col(colname)
        window = utils.generate_window(self.timecol)

        increasing, decreasing = lit(True), lit(True)
        for i in range(1, 7):
            increasing &= column > func.lag(column, i).over(window)
            decreasing &= column < func.lag(column, i).over(window)

        return increasing | decreasing

    def rule4(self, colname):
        """Nelson rule 4
        14 (or more) points in a row alternate in direction,
        increasing then decreasing
        (bimodal, 2 or more factors in data set)
        """
        column = col(colname)
        window = utils.generate_window(self.timecol)
        overall_window = utils.generate_window(self.timecol, (-14, 0))

        increasing = column > func.lag(column, 1).over(window)
        decreasing = column < func.lag(column, 1).over(window)

        xor = ((increasing & ~decreasing) | (~increasing & decreasing))
        zig = increasing & func.lag(decreasing, 1).over(window)
        zag = decreasing & func.lag(increasing, 1).over(window)

        zigzag_count = (xor & (zig | zag)).astype('int')
        zigzag = func.sum(zigzag_count).over(overall_window) >= 14

        return zigzag

    def rule5(self, colname):
        """Nelson rule 5
        2 (or 3) out of 3 points in a row are more than 2 standard deviations
        from the mean in the same direction.

        Note: actual implementation only checks for two points.
        """
        column = col(colname)
        window = utils.generate_window(self.timecol, (-2, 0))
        lower, upper = utils.get_control_range(colname, 2)

        two_below = utils.under(column, lower, window) >= 2
        two_above = utils.over(column, upper, window) >= 2

        return two_above | two_below

    def rule6(self, colname):
        """Nelson rule 6
        4 or 5 out of 5 points in a row are more than 1 standard deviation
        from the mean in the same direction.

        Note: actual implementation only checks for four points.
        """
        column = col(colname)
        window = utils.generate_window(self.timecol, (-4, 0))
        lower, upper = utils.get_control_range(colname)

        four_below = utils.under(column, lower, window) >= 4
        four_above = utils.over(column, upper, window) >= 4

        return four_above | four_below

    def rule7(self, colname):
        """Nelson rule 7
        Fifteen points in a row are all within 1 standard deviation
        of the mean on either side of the mean
        (reduced variation or measurement issue)
        """
        column = col(colname)
        window = utils.generate_window(self.timecol, (-15, 0))
        lower, upper = utils.get_control_range(colname)

        between = (column.between(lower, upper)).astype('int')
        all_in_std = func.sum(between).over(window) >=15

        return all_in_std

    def rule8(self, colname):
        """Nelson rule 8
        8 points in a row exist, but none within 1 standard deviation
        of the mean, and the points are in both directions from the mean.
        """
        column = col(colname)
        window = utils.generate_window(self.timecol, (-8, 0))
        lower, upper = utils.get_control_range(colname)

        not_between = (~column.between(lower, upper)).astype('int')
        none_in_std = func.sum(not_between).over(window) == 8

        below = utils.under(column, lower, window) >= 1
        above = utils.over(column, upper, window) >= 1

        return none_in_std & above & below
