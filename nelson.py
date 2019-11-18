from datetime.datetime import strptime

from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as func
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window


class NelsonRules:
    """Nelson rules for six sigma analysis.

    More about the rules:
    - http://leansixsigmadefinition.com/glossary/nelson-rules/
    - https://en.wikipedia.org/w/index.php?title=Nelson_rules
    """

    def __init__(self, timecol='timestamp'):
        self.timecol = timecol
        self.rules = {
            'rule1': self.rule1,
            'rule2': self.rule2,
            'rule3': self.rule3,
            'rule4': self.rule4,
            'rule5': self.rule5,
            'rule6': self.rule6,
            'nelson4': self.nelson4,
            'nelson7': self.nelson7,
        }

    def apply(self, df, colnames):
        if not isinstance(colnames, list):
            colnames = [colnames]

        for colname in colnames:
            mean, std = self.compute_stats(df, colname)
            df = (df
                  .withColumn('{}_mean'.format(colname), lit(mean))
                  .withColumn('{}_std'.format(colname), lit(std)))

            for rulename, rule in self.rules.items():
                df = df.withColumn('{}_{}'.format(colname, rulename), rule(colname))

        return df

    def generate_window(self, order_col, rowrange=None, partitions=None):
        window = Window().orderBy(order_col)

        # for future reference: if we want to use partitions
        # if partitions is not None:
        #     window = window.partitionBy(partitions)

        if rowrange is not None:
            window = window.rowsBetween(*rowrange)

        return window

    @staticmethod
    def over(column, value, window):
        return func.sum((column > value).astype('int')).over(window)

    @staticmethod
    def under(column, value, window):
        return func.sum((column < value).astype('int')).over(window)

    @staticmethod
    def get_control_range(colname, sigma=1):
        """
        Returns the lower and upper thresholds for a column (colname).
        Thresholds are the standard deviations n times (sigma) from the mean.
        """
        mean = col('{}_mean'.format(colname))
        std = col('{}_std'.format(colname))

        lower, upper = mean - sigma * std, mean + sigma * std

        return lower, upper

    @staticmethod
    def compute_stats(df, colname):
        column = col(colname)
        meancol = func.mean(column).alias('{}_mean'.format(colname))
        stdcol = func.stddev(column).alias('{}_std'.format(colname))

        results = df.select(meancol, stdcol).first()
        mean = results['{}_mean'.format(colname)]
        std = results['{}_std'.format(colname)]

        return mean, std

    def rule1(self, colname):
        """Nelson rule 1
        control limit breach - 1 point is more than 3 stddev from mean
        """
        column = col(colname)
        lower, upper = self.get_control_range(colname, 3)

        return ~column.between(lower, upper)

    def rule2(self, colname):
        """Nelson rule 2
        7 or more points are the same side of the mean (should be 9 points)
        """
        column = col(colname)
        window = self.generate_window(self.timecol, (-6, 0))

        mean = col('{}_mean'.format(colname))

        all_above = self.over(column, mean, window) == 7
        all_below = self.under(column, mean, window) == 7

        return all_above | all_below

    def rule3(self, colname):
        """Nelson rule 3
        7 or more points are increasing / decreasing (should be 6 points)
        """
        column = col(colname)
        window = self.generate_window(self.timecol)

        increasing, decreasing = lit(True), lit(True)
        for i in range(1, 7):
            increasing &= column > func.lag(column, i).over(window)
            decreasing &= column < func.lag(column, i).over(window)

        return increasing | decreasing

    def rule4(self, colname):
        """Nelson rule 5
        2 (or 3) out of 3 points in a row are more than 2 standard deviations
        from the mean in the same direction.

        Note: actual implementation only checks for two points.
        """
        column = col(colname)
        window = self.generate_window(self.timecol, (-2, 0))
        lower, upper = self.get_control_range(colname, 2)

        two_below = self.under(column, lower, window) >= 2
        two_above = self.over(column, upper, window) >= 2

        return two_above | two_below

    def rule5(self, colname):
        """Nelson rule 6
        4 or 5 out of 5 points in a row are more than 1 standard deviation
        from the mean in the same direction.

        Note: actual implementation only checks for four points.
        """
        column = col(colname)
        window = self.generate_window(self.timecol, (-4, 0))
        lower, upper = self.get_control_range(colname)

        four_below = self.under(column, lower, window) >= 4
        four_above = self.over(column, upper, window) >= 4

        return four_above | four_below

    def rule6(self, colname):
        """Nelson rule 8
        8 points in a row exist, but none within 1 standard deviation
        of the mean, and the points are in both directions from the mean.
        """
        column = col(colname)
        window = self.generate_window(self.timecol, (-8, 0))
        lower, upper = self.get_control_range(colname)

        not_between = (~column.between(lower, upper)).astype('int')
        none_in_std = func.sum(not_between).over(window) == 8

        below = self.under(column, lower, window) >= 1
        above = self.over(column, upper, window) >= 1

        return none_in_std & above & below

    def nelson4(self, colname):
      """Nelson rule 4
      14 (or more) points in a row alternate in direction,
      increasing then decreasing
      (bimodal, 2 or more factors in data set)
      """
      column = col(colname)
      window = self.generate_window(self.timecol)
      overall_window = window = self.generate_window(self.timecol, (-14, 0))

      increasing = column > func.lag(column, 1).over(window)
      decreasing = column < func.lag(column, 1).over(window)

      xor = ((increasing & ~decreasing) | (~increasing & decreasing))
      zig = increasing & func.lag(decreasing, 1).over(window)
      zag = decreasing & func.lag(increasing, 1).over(window)

      zigzag_count = (xor & (zig | zag)).astype('int')
      zigzag = func.sum(zigzag_count).over(overall_window) >= 14

      return zigzag

    def nelson7(self, colname):
      """Nelson rule 7
      Fifteen points in a row are all within 1 standard deviation
      of the mean on either side of the mean
      (reduced variation or measurement issue)
      """
      column = col(colname)
      window = self.generate_window(self.timecol, (-15, 0))
      lower, upper = self.get_control_range(colname)

      between = (column.between(lower, upper)).astype('int')
      all_in_std = func.sum(between).over(window) >=15

      return all_in_std


if __name__ == '__main__':
    sc = SparkContext(master='local[4]', appName='NelsonRules')
    spark = SparkSession(sc)

    df = spark.createDataFrame(
        data=[
            ('01', strptime('2019-01-01', '%Y-%m-%d'), 1., 11.),
            ('02', strptime('2019-01-02', '%Y-%m-%d'), 2., 10.),
            ('03', strptime('2019-01-03', '%Y-%m-%d'), 3., 9.),
            ('04', strptime('2019-01-04', '%Y-%m-%d'), 4., 8.),
            ('05', strptime('2019-01-05', '%Y-%m-%d'), 5., 7.),
            ('06', strptime('2019-01-06', '%Y-%m-%d'), 6., 6.),
            ('07', strptime('2019-01-07', '%Y-%m-%d'), 7., 5.),
            ('08', strptime('2019-01-08', '%Y-%m-%d'), 8., 6.),
            ('09', strptime('2019-01-09', '%Y-%m-%d'), 9., 7.),
            ('10', strptime('2019-01-10', '%Y-%m-%d'), 10., 8.),
            ('11', strptime('2019-01-11', '%Y-%m-%d'), 9., 9.),
            ('12', strptime('2019-01-12', '%Y-%m-%d'), 8., 10.),
            ('13', strptime('2019-01-13', '%Y-%m-%d'), 7., 11.),
            ('14', strptime('2019-01-14', '%Y-%m-%d'), 6., 10.),
            ('15', strptime('2019-01-15', '%Y-%m-%d'), 5., 9.),
            ('16', strptime('2019-01-16', '%Y-%m-%d'), 4., 8.),
            ('17', strptime('2019-01-17', '%Y-%m-%d'), 3., 7.),
            ('18', strptime('2019-01-18', '%Y-%m-%d'), 2., 6.),
            ('19', strptime('2019-01-19', '%Y-%m-%d'), 1., 5.),
            ('20', strptime('2019-01-20', '%Y-%m-%d'), 21., 1.),
            ('21', strptime('2019-01-21', '%Y-%m-%d'), -21., 1.),
            ('22', strptime('2019-01-22', '%Y-%m-%d'), 12., 1.),
            ('23', strptime('2019-01-23', '%Y-%m-%d'), -12., 1.),
            ('24', strptime('2019-01-24', '%Y-%m-%d'), 11., 9.),
            ('25', strptime('2019-01-25', '%Y-%m-%d'), -11., 10.),
        ],
        schema='id string, timestamp timestamp, val1 float, val2 float'
    )

    rules = NelsonRules()
    annotated = rules.apply(df, ['val1', 'val2'])

    print(annotated.toPandas())
