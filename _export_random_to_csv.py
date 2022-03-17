import numpy as np
import pandas as pd

np.random.seed(0)

data = {
    "height": np.around(np.random.uniform(low=60,high=75,size=1000000), decimals= 5),
    "weight": np.around(np.random.uniform(low=100,high=150,size=1000000), decimals= 4)
}

df = pd.DataFrame(data, columns=['height', 'weight'])
df.index += 25001
df.to_csv('./test_files/hw_random.csv', index=True, index_label='id')