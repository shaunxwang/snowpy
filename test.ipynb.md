```python
import pandas as pd
from snowpy import run_SQL
```


```python
df = run_SQL('./SQLs','my_sql.sql')
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>current_version</th>
      <th>current_date_time</th>
      <th>message</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4.22.3</td>
      <td>2020-07-03 13:53:35.730000+00:00</td>
      <td>Well Done!</td>
    </tr>
  </tbody>
</table>
</div>


