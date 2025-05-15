#!/usr/bin/env python
# coding: utf-8

# ## common
# 
# New notebook

# In[ ]:


version = "1.0.1"
release_date = "2025-02-27"
author = "Julian Wu"
comment = "MDD Framework"

change_history = """
--2025-02-27: 
     1. Placeholders of schema names and input view of sql transformers do not have to be included  in "<>"
     2. Row validators will automatically skip the "delete" records which are captured by CDF
--2025-02-26: move the sql transformer script to an independent sql file
--2025-02-24: optimize the yml files
-- 2024-10-20: first release
"""

bugs = """

"""


# In[ ]:


metadata_root_path_global = "/lakehouse/default/Files/metadata/"
datareaders_package_path = "./env/mdd/reader"
datalanders_package_path = "./env/mdd/lander"

