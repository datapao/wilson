from wilson.charts import NelsonRules


class SixSigma:

    @staticmethod
    def apply(df, cols, timecol='timestamp'):
        rule_mapping = SixSigma.generate_rule_mapping(df, cols)
        for Rule, columns in rule_mapping.items():
            df = Rule(timecol=timecol).apply(df, columns)
        return df

    @staticmethod
    def generate_rule_mapping(df, candidate_cols):
        mapping = {}
        for col in candidate_cols:
            Rule = SixSigma.determine(df, col)
            mapping.setdefault(Rule, []).append(col)
        return mapping

    @staticmethod
    def determine(df, col):
        # TODO: implement column checking
        return NelsonRules
