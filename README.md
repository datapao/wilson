# Six Sigma rules for PySpark DataFrames

Six sigma rule generator is a pyspark tool to generate six sigma rules for columns.
Background: https://www.isixsigma.com/tools-templates/control-charts/a-guide-to-control-charts/

The rule generator expects the target DataFrame to have a `timestamp` column.

## Installation

### For local usage:

#### 1. Clone or download repository

#### 2. Install using:
```bash
pip install -e .
```

### For DataBricks installation:

#### 1. Clone or download repository

#### 2. Generate egg file using:
```bash
python setup.py bdist
```
#### 3. Install on Databricks:
- Navigate to `Clusters`/`[your cluster]`/`Libraries` page:
- Click `Install New` button
- Select `Python Egg` from `Library Type` tab
- Drag&drop the generated .egg file from the cloned repository's `dist` directory to the window
- Click `Install` button


## Usage

```python
from ruler import RuleGenerator

df = spark.read.csv('example.csv')

rule_generator = RuleGenerator(timecol='timestamp')
df = rule_generator.apply(df, ['target_column_1'])

df.show()
```